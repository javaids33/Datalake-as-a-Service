"""
FastAPI application — lifespan startup/shutdown sequence.
Wires all components together in dependency order.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from dlsidecar.config import settings

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("dlsidecar.main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup/shutdown sequence — initialize all components in dependency order."""
    logger.info("dlsidecar starting: mode=%s tenant=%s", settings.mode, settings.tenant_id)

    # 1. DuckDB engine
    from dlsidecar.engine.duckdb_engine import DuckDBEngine

    duckdb_engine = DuckDBEngine()
    conn = duckdb_engine.startup()

    # 2. S3 source (configures httpfs on DuckDB connection)
    from dlsidecar.sources.s3 import S3Source

    s3_source = S3Source(conn=conn)
    await s3_source.connect()

    # 3. JWT manager
    from dlsidecar.auth.jwt_manager import JWTManager

    jwt_mgr = JWTManager()
    jwt_mgr.load()
    await jwt_mgr.start_refresh_loop()

    # 4. Connection registry
    from dlsidecar.connections.registry import ConnectionRegistry

    registry = ConnectionRegistry()
    registry.register(s3_source)

    # 5. Starburst source (attached as DuckDB Trino catalog)
    starburst_source = None
    if settings.starburst_enabled and settings.starburst_host:
        from dlsidecar.sources.starburst import StarburstSource

        starburst_source = StarburstSource(duckdb_engine=duckdb_engine)
        jwt_mgr.on_refresh(starburst_source.refresh_jwt)
        registry.register(starburst_source)

    # 5b. Additional DuckDB-native datasource attachments
    if settings.postgres_enabled and settings.postgres_dsn:
        duckdb_engine.attach_datasource("postgres", settings.postgres_dsn, "POSTGRES")
    if settings.mysql_enabled and settings.mysql_dsn:
        duckdb_engine.attach_datasource("mysql", settings.mysql_dsn, "MYSQL")

    # 6. HMS source
    hms_source = None
    if settings.hms_enabled:
        from dlsidecar.sources.hms import HMSSource

        hms_source = HMSSource()
        registry.register(hms_source)

    # 7. Start all connections
    await registry.startup()

    # 8. Query router and cache
    from dlsidecar.engine.cache import QueryCache
    from dlsidecar.engine.query_router import QueryRouter

    query_router = QueryRouter()
    query_cache = QueryCache()

    # 9. Iceberg manager
    iceberg_mgr = None
    iceberg_maintenance = None
    iceberg_health = None
    if settings.iceberg_enabled:
        from dlsidecar.iceberg.health import IcebergHealth
        from dlsidecar.iceberg.maintenance import IcebergMaintenance
        from dlsidecar.iceberg.manager import IcebergManager

        iceberg_mgr = IcebergManager()
        iceberg_mgr.set_hms(hms_source)
        iceberg_maintenance = IcebergMaintenance()
        iceberg_maintenance.start()
        iceberg_health = IcebergHealth(hms_source=hms_source, duckdb_engine=duckdb_engine)

    # 10. Streaming ingest
    from dlsidecar.streaming.ingest import IngestManager

    ingest_mgr = IngestManager(duckdb_engine=duckdb_engine, iceberg_manager=iceberg_mgr)

    # 11. Kafka bridge
    from dlsidecar.streaming.kafka_bridge import KafkaBridge

    kafka_bridge = KafkaBridge(iceberg_manager=iceberg_mgr)
    await kafka_bridge.start()

    # 12. Wire API modules
    from dlsidecar.api import catalog, health, iceberg, push, query, sources, stream

    health.init(registry, duckdb_engine)
    query.init(duckdb_engine, query_router, query_cache)
    push.init(iceberg_mgr, duckdb_engine)
    stream.init(ingest_mgr)
    catalog.init(registry, duckdb_engine)
    sources.init(registry)
    iceberg.init(iceberg_health, iceberg_maintenance)

    # 13. Arrow Flight server
    from dlsidecar.flight.server import start_flight_server

    flight_server = start_flight_server(
        duckdb_engine=duckdb_engine,
        query_router=query_router,
        iceberg_manager=iceberg_mgr,
    )

    logger.info("dlsidecar ready: rest=%d flight=%d metrics=%d", settings.rest_port, settings.flight_port, settings.metrics_port)

    yield

    # Shutdown
    logger.info("dlsidecar shutting down")
    await kafka_bridge.stop()
    if iceberg_maintenance:
        iceberg_maintenance.stop()
    flight_server.shutdown()
    await jwt_mgr.stop()
    await registry.shutdown()
    duckdb_engine.shutdown()
    logger.info("dlsidecar stopped")


# Create FastAPI app
app = FastAPI(
    title="dlsidecar",
    description="Data Lake as a Service sidecar",
    version="0.1.0",
    lifespan=lifespan,
)

# Register routers
from dlsidecar.api.catalog import router as catalog_router
from dlsidecar.api.health import router as health_router
from dlsidecar.api.iceberg import router as iceberg_router
from dlsidecar.api.metrics import router as metrics_router
from dlsidecar.api.push import router as push_router
from dlsidecar.api.query import router as query_router
from dlsidecar.api.sources import router as sources_router
from dlsidecar.api.stream import router as stream_router
from dlsidecar.api.healthcheck import router as healthcheck_router

app.include_router(health_router)
app.include_router(query_router)
app.include_router(push_router)
app.include_router(stream_router)
app.include_router(catalog_router)
app.include_router(sources_router)
app.include_router(iceberg_router)
app.include_router(metrics_router)
app.include_router(healthcheck_router)


# Onboarding API endpoints (dev mode)
if settings.ui_enabled:
    from dlsidecar.onboarding.egress_generator import generate_egress_rules, generate_network_policy_yaml
    from dlsidecar.onboarding.promoter import classify_vars, get_config_dict, validate_promotion
    from dlsidecar.onboarding.validator import validate_all

    @app.get("/api/onboarding/config")
    async def onboarding_config():
        return get_config_dict()

    @app.get("/api/onboarding/classify")
    async def onboarding_classify():
        return classify_vars()

    @app.get("/api/onboarding/egress")
    async def onboarding_egress():
        return {"rules": generate_egress_rules()}

    @app.get("/api/onboarding/egress/yaml")
    async def onboarding_egress_yaml():
        return {"yaml": generate_network_policy_yaml()}

    @app.post("/api/onboarding/validate")
    async def onboarding_validate():
        results = await validate_all(settings)
        return results

    @app.post("/api/onboarding/promote/validate")
    async def onboarding_promote_validate(body: dict):
        return {"issues": validate_promotion(body)}

    @app.post("/api/onboarding/explain")
    async def onboarding_explain(body: dict):
        from dlsidecar.engine.query_router import QueryRouter
        router = QueryRouter()
        plan = router.route(body.get("sql", ""))
        return {
            "engine": plan.engine.value,
            "routing_reason": ",".join(r.value for r in plan.reasons),
            "tables_owned": plan.tables_owned,
            "tables_unowned": plan.tables_unowned,
            "join_strategy": plan.join_strategy.value,
        }

    # Streamlit UI is launched separately:
    #   streamlit run src/dlsidecar/onboarding/ui.py --server.port 3000


def main():
    """Entry point for running the sidecar."""
    # If UI is enabled, launch Streamlit in a background thread
    if settings.ui_enabled:
        import subprocess
        import threading
        import os

        ui_path = os.path.join(os.path.dirname(__file__), "onboarding", "ui.py")

        def run_streamlit():
            subprocess.run(
                ["streamlit", "run", ui_path,
                 "--server.port", str(settings.onboarding_ui_port),
                 "--server.headless", "true",
                 "--browser.gatherUsageStats", "false"],
                cwd=os.path.dirname(ui_path),
            )

        thread = threading.Thread(target=run_streamlit, daemon=True)
        thread.start()
        logger.info("Streamlit UI started on port %d", settings.onboarding_ui_port)

    uvicorn.run(
        "dlsidecar.main:app",
        host="0.0.0.0",
        port=settings.rest_port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
