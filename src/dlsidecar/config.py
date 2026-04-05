"""
dlsidecar configuration — pydantic-settings with full env var schema,
preset loader, and EnvVarRole classification for onboarding UI.
"""

from __future__ import annotations

import json
from enum import Enum
from typing import Any

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class EnvVarRole(str, Enum):
    """Classification of every env var for the onboarding UI config generator."""

    CORE = "core"  # Always required
    SECRET = "secret"  # Must be injected via ESO / Vault — never in ConfigMap
    ENV_SPECIFIC = "env_specific"  # Value changes per environment (dev/staging/prod)
    OPTIONAL = "optional"  # Has a sensible default
    FEATURE_FLAG = "feature_flag"  # Toggles a capability on/off


# Maps every env var name to its role for the onboarding UI
ENV_VAR_ROLES: dict[str, EnvVarRole] = {
    # Core
    "DLS_MODE": EnvVarRole.ENV_SPECIFIC,
    "DLS_TENANT_ID": EnvVarRole.CORE,
    "DLS_OWNED_NAMESPACES": EnvVarRole.CORE,
    "DLS_ROW_FILTER": EnvVarRole.OPTIONAL,
    # S3
    "DLS_S3_BUCKETS": EnvVarRole.CORE,
    "DLS_S3_REGION": EnvVarRole.ENV_SPECIFIC,
    "DLS_S3_ENDPOINT_URL": EnvVarRole.ENV_SPECIFIC,
    "DLS_S3_PATH_STYLE": EnvVarRole.OPTIONAL,
    "DLS_BUCKET_CONFIG": EnvVarRole.OPTIONAL,
    # Starburst
    "DLS_STARBURST_ENABLED": EnvVarRole.FEATURE_FLAG,
    "DLS_STARBURST_HOST": EnvVarRole.ENV_SPECIFIC,
    "DLS_STARBURST_PORT": EnvVarRole.ENV_SPECIFIC,
    "DLS_STARBURST_CATALOG": EnvVarRole.OPTIONAL,
    "DLS_STARBURST_SCHEMA": EnvVarRole.OPTIONAL,
    "DLS_STARBURST_AUTH_MODE": EnvVarRole.OPTIONAL,
    "DLS_STARBURST_JWT_TOKEN_PATH": EnvVarRole.ENV_SPECIFIC,
    "DLS_STARBURST_TLS_ENABLED": EnvVarRole.ENV_SPECIFIC,
    "DLS_STARBURST_TLS_CERT_PATH": EnvVarRole.ENV_SPECIFIC,
    "DLS_STARBURST_CONNECTION_POOL_SIZE": EnvVarRole.OPTIONAL,
    "DLS_STARBURST_QUERY_TIMEOUT": EnvVarRole.OPTIONAL,
    "DLS_STARBURST_PUSHDOWN_ENABLED": EnvVarRole.OPTIONAL,
    "DLS_STARBURST_RESULTS_CACHE_TTL": EnvVarRole.OPTIONAL,
    # HMS
    "DLS_HMS_ENABLED": EnvVarRole.FEATURE_FLAG,
    "DLS_HMS_URI": EnvVarRole.ENV_SPECIFIC,
    "DLS_HMS_DATABASE": EnvVarRole.OPTIONAL,
    "DLS_HMS_AUTH": EnvVarRole.OPTIONAL,
    # DuckDB
    "DLS_DUCKDB_MODE": EnvVarRole.OPTIONAL,
    "DLS_DUCKDB_PATH": EnvVarRole.OPTIONAL,
    "DLS_DUCKDB_MEMORY_LIMIT": EnvVarRole.OPTIONAL,
    "DLS_DUCKDB_THREADS": EnvVarRole.OPTIONAL,
    "DLS_DUCKDB_EXTENSIONS": EnvVarRole.OPTIONAL,
    "DLS_DUCKDB_SHARED_FILE_PATH": EnvVarRole.OPTIONAL,
    "DLS_DIRECT_SCAN_THRESHOLD": EnvVarRole.OPTIONAL,
    # Iceberg
    "DLS_ICEBERG_ENABLED": EnvVarRole.FEATURE_FLAG,
    "DLS_ICEBERG_CATALOG_TYPE": EnvVarRole.CORE,
    "DLS_ICEBERG_CATALOG_URI": EnvVarRole.ENV_SPECIFIC,
    "DLS_ICEBERG_WAREHOUSE": EnvVarRole.ENV_SPECIFIC,
    "DLS_ICEBERG_NAMESPACE": EnvVarRole.OPTIONAL,
    "DLS_ICEBERG_GLUE_CATALOG_ID": EnvVarRole.ENV_SPECIFIC,
    "DLS_ICEBERG_TARGET_FILE_SIZE_MB": EnvVarRole.OPTIONAL,
    "DLS_ICEBERG_COMPACTION_ENABLED": EnvVarRole.FEATURE_FLAG,
    "DLS_ICEBERG_COMPACTION_CRON": EnvVarRole.OPTIONAL,
    "DLS_ICEBERG_SNAPSHOT_EXPIRY_ENABLED": EnvVarRole.FEATURE_FLAG,
    "DLS_ICEBERG_SNAPSHOT_EXPIRY_CRON": EnvVarRole.OPTIONAL,
    "DLS_ICEBERG_SNAPSHOT_MIN_AGE_HOURS": EnvVarRole.OPTIONAL,
    "DLS_ICEBERG_SNAPSHOT_MIN_COUNT": EnvVarRole.OPTIONAL,
    "DLS_ICEBERG_ORPHAN_CLEANUP_ENABLED": EnvVarRole.FEATURE_FLAG,
    "DLS_ICEBERG_ORPHAN_CLEANUP_CRON": EnvVarRole.OPTIONAL,
    "DLS_ICEBERG_ORPHAN_RETENTION_HOURS": EnvVarRole.OPTIONAL,
    "DLS_ICEBERG_MANIFEST_REWRITE_ENABLED": EnvVarRole.FEATURE_FLAG,
    "DLS_ICEBERG_MANIFEST_REWRITE_CRON": EnvVarRole.OPTIONAL,
    "DLS_ICEBERG_MAX_MANIFESTS": EnvVarRole.OPTIONAL,
    # JWT / Auth
    "DLS_JWT_TOKEN_PATH": EnvVarRole.ENV_SPECIFIC,
    "DLS_JWT_REFRESH_BEFORE_EXPIRY_SECONDS": EnvVarRole.OPTIONAL,
    "DLS_JWT_JWKS_URI": EnvVarRole.ENV_SPECIFIC,
    "DLS_JWT_AUDIENCE": EnvVarRole.OPTIONAL,
    "DLS_AUTH_REFRESH_URL": EnvVarRole.ENV_SPECIFIC,
    # Streaming
    "DLS_STREAM_BUFFER_ROWS": EnvVarRole.OPTIONAL,
    # Kafka
    "DLS_KAFKA_ENABLED": EnvVarRole.FEATURE_FLAG,
    "DLS_KAFKA_BROKERS": EnvVarRole.ENV_SPECIFIC,
    "DLS_KAFKA_CONSUMER_TOPICS": EnvVarRole.OPTIONAL,
    "DLS_KAFKA_CONSUMER_GROUP": EnvVarRole.OPTIONAL,
    "DLS_KAFKA_FORMAT": EnvVarRole.OPTIONAL,
    "DLS_KAFKA_ICEBERG_TARGET": EnvVarRole.OPTIONAL,
    "DLS_KAFKA_SCHEMA_REGISTRY_URL": EnvVarRole.ENV_SPECIFIC,
    "DLS_KAFKA_FLUSH_INTERVAL_SECONDS": EnvVarRole.OPTIONAL,
    "DLS_KAFKA_BATCH_SIZE": EnvVarRole.OPTIONAL,
    # External sources
    "DLS_SNOWFLAKE_ENABLED": EnvVarRole.FEATURE_FLAG,
    "DLS_SNOWFLAKE_ACCOUNT": EnvVarRole.ENV_SPECIFIC,
    "DLS_SNOWFLAKE_USER": EnvVarRole.SECRET,
    "DLS_SNOWFLAKE_DATABASE": EnvVarRole.ENV_SPECIFIC,
    "DLS_SNOWFLAKE_SCHEMA": EnvVarRole.OPTIONAL,
    "DLS_SNOWFLAKE_WAREHOUSE": EnvVarRole.ENV_SPECIFIC,
    "DLS_SNOWFLAKE_ROLE": EnvVarRole.OPTIONAL,
    "DLS_DATABRICKS_ENABLED": EnvVarRole.FEATURE_FLAG,
    "DLS_DATABRICKS_HOST": EnvVarRole.ENV_SPECIFIC,
    "DLS_DATABRICKS_HTTP_PATH": EnvVarRole.ENV_SPECIFIC,
    "DLS_DATABRICKS_CATALOG": EnvVarRole.OPTIONAL,
    "DLS_POSTGRES_ENABLED": EnvVarRole.FEATURE_FLAG,
    "DLS_POSTGRES_DSN": EnvVarRole.SECRET,
    "DLS_MYSQL_ENABLED": EnvVarRole.FEATURE_FLAG,
    "DLS_MYSQL_DSN": EnvVarRole.SECRET,
    "DLS_MONGO_ENABLED": EnvVarRole.FEATURE_FLAG,
    "DLS_MONGO_URI": EnvVarRole.SECRET,
    "DLS_DELTA_ENABLED": EnvVarRole.FEATURE_FLAG,
    "DLS_DELTA_TABLE_PATHS": EnvVarRole.OPTIONAL,
    "DLS_AZURE_ENABLED": EnvVarRole.FEATURE_FLAG,
    "DLS_AZURE_STORAGE_ACCOUNT": EnvVarRole.ENV_SPECIFIC,
    "DLS_AZURE_CONTAINER": EnvVarRole.ENV_SPECIFIC,
    "DLS_GCS_ENABLED": EnvVarRole.FEATURE_FLAG,
    "DLS_GCS_BUCKET": EnvVarRole.ENV_SPECIFIC,
    "DLS_GCS_PROJECT": EnvVarRole.ENV_SPECIFIC,
    "DLS_EXTERNAL_LAKE_ENABLED": EnvVarRole.FEATURE_FLAG,
    "DLS_EXTERNAL_LAKE_HMS_URI": EnvVarRole.ENV_SPECIFIC,
    "DLS_EXTERNAL_LAKE_CATALOG_URI": EnvVarRole.ENV_SPECIFIC,
    "DLS_EXTERNAL_LAKE_S3_PREFIX": EnvVarRole.ENV_SPECIFIC,
    # API
    "DLS_REST_PORT": EnvVarRole.OPTIONAL,
    "DLS_FLIGHT_PORT": EnvVarRole.OPTIONAL,
    "DLS_METRICS_PORT": EnvVarRole.OPTIONAL,
    "DLS_UNIX_SOCKET_PATH": EnvVarRole.OPTIONAL,
    "DLS_MAX_RESULT_ROWS": EnvVarRole.OPTIONAL,
    "DLS_QUERY_TIMEOUT_SECONDS": EnvVarRole.OPTIONAL,
    # Onboarding UI
    "DLS_ONBOARDING_UI_ENABLED": EnvVarRole.FEATURE_FLAG,
    "DLS_ONBOARDING_UI_PORT": EnvVarRole.OPTIONAL,
    # MotherDuck
    "DLS_MOTHERDUCK_ENABLED": EnvVarRole.FEATURE_FLAG,
    "DLS_MOTHERDUCK_TOKEN": EnvVarRole.SECRET,
    "DLS_MOTHERDUCK_DATABASE": EnvVarRole.ENV_SPECIFIC,
    "DLS_SCALE_THRESHOLD_MEMORY_PERCENT": EnvVarRole.OPTIONAL,
    "DLS_SCALE_THRESHOLD_QUERY_BYTES": EnvVarRole.OPTIONAL,
    # Observability
    "DLS_LOG_LEVEL": EnvVarRole.OPTIONAL,
    "DLS_ENABLE_QUERY_LOG": EnvVarRole.FEATURE_FLAG,
    "DLS_ENABLE_METRICS": EnvVarRole.FEATURE_FLAG,
    "DLS_OTLP_ENDPOINT": EnvVarRole.ENV_SPECIFIC,
}


class Settings(BaseSettings):
    """All dlsidecar configuration, loaded from environment variables prefixed DLS_."""

    model_config = SettingsConfigDict(
        env_prefix="DLS_",
        env_nested_delimiter="__",
        case_sensitive=False,
    )

    # ── Core ──────────────────────────────────────────────────────────────────
    mode: str = Field(default="dev", description="Runtime mode: dev | staging | prod")
    tenant_id: str = Field(default="default", description="Tenant identifier for audit + metrics")
    owned_namespaces: str = Field(default="", description="Comma-separated HMS namespaces this pod owns")
    row_filter: str = Field(default="", description="SQL predicate injected into all DuckDB queries")

    # ── S3 / Object storage ───────────────────────────────────────────────────
    s3_buckets: str = Field(default="", description="Comma-separated list of team S3 bucket names")
    s3_region: str = Field(default="us-east-1")
    s3_endpoint_url: str = Field(default="", description="MinIO/on-prem S3 endpoint")
    s3_path_style: bool = Field(default=False)
    bucket_config: str = Field(default="{}", description="JSON: per-bucket region/endpoint overrides")

    # ── Starburst ─────────────────────────────────────────────────────────────
    starburst_enabled: bool = Field(default=True)
    starburst_host: str = Field(default="")
    starburst_port: int = Field(default=8443)
    starburst_catalog: str = Field(default="iceberg")
    starburst_schema: str = Field(default="default")
    starburst_auth_mode: str = Field(default="jwt", description="jwt | ldap | oauth2")
    starburst_jwt_token_path: str = Field(default="/var/run/secrets/dlsidecar/jwt")
    starburst_tls_enabled: bool = Field(default=True)
    starburst_tls_cert_path: str = Field(default="")
    starburst_connection_pool_size: int = Field(default=10)
    starburst_query_timeout: int = Field(default=600)
    starburst_pushdown_enabled: bool = Field(default=True)
    starburst_results_cache_ttl: int = Field(default=300)

    # ── HMS ───────────────────────────────────────────────────────────────────
    hms_enabled: bool = Field(default=True)
    hms_uri: str = Field(default="thrift://hive-metastore:9083")
    hms_database: str = Field(default="default")
    hms_auth: str = Field(default="NOSASL")

    # ── DuckDB ────────────────────────────────────────────────────────────────
    duckdb_mode: str = Field(default="memory", description="memory | persistent")
    duckdb_path: str = Field(default="/data/dlsidecar.duckdb")
    duckdb_memory_limit: str = Field(default="4GB")
    duckdb_threads: int = Field(default=4)
    duckdb_extensions: str = Field(
        default="httpfs,iceberg,json,parquet",
        description="Comma-separated DuckDB extensions to load at startup",
    )
    duckdb_shared_file_path: str = Field(default="/shared/result.arrow")
    direct_scan_threshold: int = Field(default=53687091200, description="50GB in bytes")

    # ── Iceberg ───────────────────────────────────────────────────────────────
    iceberg_enabled: bool = Field(default=True)
    iceberg_catalog_type: str = Field(default="glue", description="glue | rest | hive")
    iceberg_catalog_uri: str = Field(default="")
    iceberg_warehouse: str = Field(default="")
    iceberg_namespace: str = Field(default="default")
    iceberg_glue_catalog_id: str = Field(default="")
    iceberg_target_file_size_mb: int = Field(default=128)
    iceberg_compaction_enabled: bool = Field(default=True)
    iceberg_compaction_cron: str = Field(default="0 2 * * *")
    iceberg_snapshot_expiry_enabled: bool = Field(default=True)
    iceberg_snapshot_expiry_cron: str = Field(default="0 3 * * *")
    iceberg_snapshot_min_age_hours: int = Field(default=168)
    iceberg_snapshot_min_count: int = Field(default=5)
    iceberg_orphan_cleanup_enabled: bool = Field(default=True)
    iceberg_orphan_cleanup_cron: str = Field(default="0 4 * * *")
    iceberg_orphan_retention_hours: int = Field(default=72)
    iceberg_manifest_rewrite_enabled: bool = Field(default=True)
    iceberg_manifest_rewrite_cron: str = Field(default="0 5 * * *")
    iceberg_max_manifests: int = Field(default=100)

    # ── JWT / Auth ────────────────────────────────────────────────────────────
    jwt_token_path: str = Field(default="/var/run/secrets/dlsidecar/jwt")
    jwt_refresh_before_expiry_seconds: int = Field(default=300)
    jwt_jwks_uri: str = Field(default="")
    jwt_audience: str = Field(default="starburst,dlsidecar")
    auth_refresh_url: str = Field(default="")

    # ── Streaming ─────────────────────────────────────────────────────────────
    stream_buffer_rows: int = Field(default=100000)

    # ── Kafka ─────────────────────────────────────────────────────────────────
    kafka_enabled: bool = Field(default=False)
    kafka_brokers: str = Field(default="")
    kafka_consumer_topics: str = Field(default="")
    kafka_consumer_group: str = Field(default="")
    kafka_format: str = Field(default="json", description="json | avro | protobuf")
    kafka_iceberg_target: str = Field(default="")
    kafka_schema_registry_url: str = Field(default="")
    kafka_flush_interval_seconds: int = Field(default=60)
    kafka_batch_size: int = Field(default=10000)

    # ── External sources ──────────────────────────────────────────────────────
    snowflake_enabled: bool = Field(default=False)
    snowflake_account: str = Field(default="")
    snowflake_user: str = Field(default="")
    snowflake_database: str = Field(default="")
    snowflake_schema: str = Field(default="")
    snowflake_warehouse: str = Field(default="")
    snowflake_role: str = Field(default="")

    databricks_enabled: bool = Field(default=False)
    databricks_host: str = Field(default="")
    databricks_http_path: str = Field(default="")
    databricks_catalog: str = Field(default="")

    postgres_enabled: bool = Field(default=False)
    postgres_dsn: str = Field(default="")

    mysql_enabled: bool = Field(default=False)
    mysql_dsn: str = Field(default="")

    mongo_enabled: bool = Field(default=False)
    mongo_uri: str = Field(default="")

    delta_enabled: bool = Field(default=False)
    delta_table_paths: str = Field(default="")

    azure_enabled: bool = Field(default=False)
    azure_storage_account: str = Field(default="")
    azure_container: str = Field(default="")

    gcs_enabled: bool = Field(default=False)
    gcs_bucket: str = Field(default="")
    gcs_project: str = Field(default="")

    external_lake_enabled: bool = Field(default=False)
    external_lake_hms_uri: str = Field(default="")
    external_lake_catalog_uri: str = Field(default="")
    external_lake_s3_prefix: str = Field(default="")

    # ── API ───────────────────────────────────────────────────────────────────
    rest_port: int = Field(default=8765)
    flight_port: int = Field(default=8766)
    metrics_port: int = Field(default=9090)
    unix_socket_path: str = Field(default="/tmp/dlsidecar.sock")
    max_result_rows: int = Field(default=100000)
    query_timeout_seconds: int = Field(default=300)

    # ── Onboarding UI ────────────────────────────────────────────────────────
    onboarding_ui_enabled: bool = Field(default=False)
    onboarding_ui_port: int = Field(default=3000)

    # ── MotherDuck ────────────────────────────────────────────────────────────
    motherduck_enabled: bool = Field(default=False)
    motherduck_token: str = Field(default="")
    motherduck_database: str = Field(default="")
    scale_threshold_memory_percent: int = Field(default=80)
    scale_threshold_query_bytes: int = Field(default=10737418240)

    # ── Observability ─────────────────────────────────────────────────────────
    log_level: str = Field(default="INFO")
    enable_query_log: bool = Field(default=True)
    enable_metrics: bool = Field(default=True)
    otlp_endpoint: str = Field(default="")

    # ── Derived / computed ────────────────────────────────────────────────────

    @property
    def owned_namespace_set(self) -> set[str]:
        return {ns.strip() for ns in self.owned_namespaces.split(",") if ns.strip()}

    @property
    def s3_bucket_list(self) -> list[str]:
        return [b.strip() for b in self.s3_buckets.split(",") if b.strip()]

    @property
    def bucket_config_parsed(self) -> dict[str, Any]:
        return json.loads(self.bucket_config)

    @property
    def extension_list(self) -> list[str]:
        return [e.strip() for e in self.duckdb_extensions.split(",") if e.strip()]

    @property
    def jwt_audience_list(self) -> list[str]:
        return [a.strip() for a in self.jwt_audience.split(",") if a.strip()]

    @property
    def is_dev(self) -> bool:
        return self.mode == "dev"

    @property
    def ui_enabled(self) -> bool:
        return self.onboarding_ui_enabled or self.is_dev

    @field_validator("mode")
    @classmethod
    def validate_mode(cls, v: str) -> str:
        if v not in ("dev", "staging", "prod"):
            raise ValueError(f"DLS_MODE must be dev|staging|prod, got '{v}'")
        return v

    @field_validator("duckdb_mode")
    @classmethod
    def validate_duckdb_mode(cls, v: str) -> str:
        if v not in ("memory", "persistent"):
            raise ValueError(f"DLS_DUCKDB_MODE must be memory|persistent, got '{v}'")
        return v

    @field_validator("iceberg_catalog_type")
    @classmethod
    def validate_iceberg_catalog_type(cls, v: str) -> str:
        if v not in ("glue", "rest", "hive"):
            raise ValueError(f"DLS_ICEBERG_CATALOG_TYPE must be glue|rest|hive, got '{v}'")
        return v

    @field_validator("starburst_auth_mode")
    @classmethod
    def validate_starburst_auth_mode(cls, v: str) -> str:
        if v not in ("jwt", "ldap", "oauth2"):
            raise ValueError(f"DLS_STARBURST_AUTH_MODE must be jwt|ldap|oauth2, got '{v}'")
        return v

    @field_validator("kafka_format")
    @classmethod
    def validate_kafka_format(cls, v: str) -> str:
        if v not in ("json", "avro", "protobuf"):
            raise ValueError(f"DLS_KAFKA_FORMAT must be json|avro|protobuf, got '{v}'")
        return v

    @model_validator(mode="after")
    def validate_cross_field(self) -> "Settings":
        if self.starburst_enabled and not self.starburst_host and self.mode != "dev":
            raise ValueError("DLS_STARBURST_HOST is required when Starburst is enabled in non-dev mode")
        if self.iceberg_enabled and self.iceberg_catalog_type == "glue" and not self.iceberg_glue_catalog_id and self.mode != "dev":
            raise ValueError("DLS_ICEBERG_GLUE_CATALOG_ID is required when using Glue catalog in non-dev mode")
        return self


# ── Presets ───────────────────────────────────────────────────────────────────
# A preset is a named dict of env var overrides. The onboarding UI and Helm chart
# use these to give teams a 3-line quickstart.

PRESETS: dict[str, dict[str, Any]] = {
    "minimal-readonly": {
        "starburst_enabled": False,
        "hms_enabled": False,
        "iceberg_enabled": False,
        "kafka_enabled": False,
        "duckdb_mode": "memory",
    },
    "full-readwrite": {
        "starburst_enabled": True,
        "hms_enabled": True,
        "iceberg_enabled": True,
        "iceberg_compaction_enabled": True,
        "iceberg_snapshot_expiry_enabled": True,
        "iceberg_orphan_cleanup_enabled": True,
        "iceberg_manifest_rewrite_enabled": True,
    },
    "streaming-ingest": {
        "starburst_enabled": True,
        "hms_enabled": True,
        "iceberg_enabled": True,
        "kafka_enabled": True,
        "stream_buffer_rows": 100000,
        "kafka_flush_interval_seconds": 60,
    },
    "dev-local": {
        "mode": "dev",
        "starburst_enabled": False,
        "hms_enabled": False,
        "iceberg_enabled": False,
        "duckdb_mode": "memory",
        "onboarding_ui_enabled": True,
    },
}


def load_settings(preset: str | None = None, **overrides: Any) -> Settings:
    """Load settings, optionally applying a preset before env vars and overrides."""
    kwargs: dict[str, Any] = {}
    if preset and preset in PRESETS:
        kwargs.update(PRESETS[preset])
    kwargs.update(overrides)
    return Settings(**kwargs)


# Module-level singleton — import this everywhere
settings = Settings()
