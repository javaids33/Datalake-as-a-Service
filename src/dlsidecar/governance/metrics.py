"""
Prometheus metric definitions for dlsidecar.
All metrics are defined here and imported by the modules that increment them.
"""

from prometheus_client import Counter, Gauge, Histogram

# ── Query metrics ─────────────────────────────────────────────────────────────

QUERIES_TOTAL = Counter(
    "dlsidecar_queries_total",
    "Total queries executed",
    ["engine", "status", "tenant"],
)

QUERY_DURATION = Histogram(
    "dlsidecar_query_duration_seconds",
    "Query execution duration in seconds",
    ["engine", "tenant"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300),
)

ROWS_SCANNED_TOTAL = Counter(
    "dlsidecar_rows_scanned_total",
    "Total rows scanned across all queries",
    ["engine", "tenant"],
)

BYTES_SCANNED_TOTAL = Counter(
    "dlsidecar_bytes_scanned_total",
    "Total bytes scanned across all queries",
    ["engine", "tenant"],
)

QUERY_CACHE_HITS = Counter(
    "dlsidecar_query_cache_hits_total",
    "Query result cache hits",
    ["tenant"],
)

QUERY_CACHE_MISSES = Counter(
    "dlsidecar_query_cache_misses_total",
    "Query result cache misses",
    ["tenant"],
)

ROUTING_DECISIONS = Counter(
    "dlsidecar_routing_decisions_total",
    "Query routing decisions",
    ["path", "reason"],
)

# ── Write / stream metrics ────────────────────────────────────────────────────

WRITES_TOTAL = Counter(
    "dlsidecar_writes_total",
    "Total write operations",
    ["format", "mode", "tenant"],
)

BYTES_WRITTEN_TOTAL = Counter(
    "dlsidecar_bytes_written_total",
    "Total bytes written",
    ["format", "tenant"],
)

ROWS_WRITTEN_TOTAL = Counter(
    "dlsidecar_rows_written_total",
    "Total rows written",
    ["format", "tenant"],
)

STREAM_BATCHES_TOTAL = Counter(
    "dlsidecar_stream_batches_total",
    "Total stream batches received",
    ["tenant"],
)

ICEBERG_COMMITS_TOTAL = Counter(
    "dlsidecar_iceberg_commits_total",
    "Total Iceberg snapshot commits",
    ["tenant"],
)

# ── Iceberg maintenance metrics ──────────────────────────────────────────────

ICEBERG_COMPACTION_RUNS = Counter(
    "dlsidecar_iceberg_compaction_runs_total",
    "Compaction runs",
    ["table", "status"],
)

ICEBERG_FILES_COMPACTED = Counter(
    "dlsidecar_iceberg_files_compacted_total",
    "Files compacted",
    ["table"],
)

ICEBERG_SNAPSHOTS_EXPIRED = Counter(
    "dlsidecar_iceberg_snapshots_expired_total",
    "Snapshots expired",
    ["table"],
)

ICEBERG_ORPHANS_DELETED = Counter(
    "dlsidecar_iceberg_orphans_deleted_total",
    "Orphan files deleted",
    ["table"],
)

ICEBERG_SMALL_FILES = Gauge(
    "dlsidecar_iceberg_small_files_gauge",
    "Current count of small files per table",
    ["table"],
)

ICEBERG_AVG_FILE_SIZE_MB = Gauge(
    "dlsidecar_iceberg_avg_file_size_mb_gauge",
    "Average data file size in MB per table",
    ["table"],
)

# ── Connection health ─────────────────────────────────────────────────────────

CONNECTION_STATUS = Gauge(
    "dlsidecar_connection_status",
    "Connection health (1=healthy, 0=degraded)",
    ["source"],
)

CONNECTION_RECONNECTS = Counter(
    "dlsidecar_connection_reconnects_total",
    "Total reconnection attempts",
    ["source"],
)

DUCKDB_MEMORY_BYTES = Gauge(
    "dlsidecar_duckdb_memory_bytes_gauge",
    "DuckDB current memory usage in bytes",
)

JWT_EXPIRY_SECONDS = Gauge(
    "dlsidecar_jwt_expiry_seconds_gauge",
    "Seconds until JWT token expires",
)

# ── Governance ────────────────────────────────────────────────────────────────

CROSS_TEAM_QUERIES = Counter(
    "dlsidecar_cross_team_queries_total",
    "Cross-team queries",
    ["tenant", "target_catalog"],
)

GOVERNANCE_BLOCKS = Counter(
    "dlsidecar_governance_blocks_total",
    "Blocked unauthorized access attempts",
    ["reason"],
)

SCHEMA_EVOLUTIONS = Counter(
    "dlsidecar_schema_evolutions_total",
    "Schema evolution events",
    ["table"],
)
