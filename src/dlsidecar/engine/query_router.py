"""
Query router — the core routing decision boundary.

THE FUNDAMENTAL ARCHITECTURE:
  DuckDB is the lightweight in-memory client that lives in the pod.
  Starburst is the distributed compute engine that does the heavy lifting.

  DuckDB excels at: reading local files, preparing/transforming data,
  caching results, scanning the team's own S3 bucket via IRSA.

  Starburst excels at: distributed joins across petabytes, cross-team
  governed federation, row-level security, heavy analytical workloads.

Uses sqlglot to parse SQL AST, extract table references, resolve them
against DLS_OWNED_NAMESPACES and DLS_S3_BUCKETS, and emit an ExecutionPlan.

ROUTING PATHS:
  PATH A — DuckDB local (lightweight, in-pod)
    Local CSV/Parquet/Arrow files, small owned-bucket scans, data prep

  PATH B — Starburst compute (distributed, heavy)
    Cross-team queries, large scans, complex analytics, governed access

  PATH C — Hybrid relay (DuckDB orchestrates, Starburst computes)
    DuckDB sends query to Starburst, receives results, caches/transforms
    locally. DuckDB is the relay — Starburst is cheffing the answers.

  PATH D — DuckDB pushdown to Starburst
    DuckDB connects to Starburst directly to offload heavy work.
    The query runs entirely on Starburst's distributed engine.
    DuckDB just relays the results back to the caller.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import sqlglot
from sqlglot import exp

from dlsidecar.config import settings
from dlsidecar.governance.audit import emit_audit_event
from dlsidecar.governance.metrics import ROUTING_DECISIONS

logger = logging.getLogger("dlsidecar.engine.query_router")

# Hint patterns: /*+ ENGINE(starburst) */ or /*+ SNAPSHOT(123) */ or /*+ AS_OF('...') */
ENGINE_HINT_RE = re.compile(r"/\*\+\s*ENGINE\s*\(\s*(\w+)\s*\)\s*\*/", re.IGNORECASE)
SNAPSHOT_HINT_RE = re.compile(r"/\*\+\s*SNAPSHOT\s*\(\s*(\d+)\s*\)\s*\*/", re.IGNORECASE)
AS_OF_HINT_RE = re.compile(r"/\*\+\s*AS_OF\s*\(\s*'([^']+)'\s*\)\s*\*/", re.IGNORECASE)

# Patterns that indicate heavy compute (aggregation, window functions, complex joins)
HEAVY_COMPUTE_PATTERNS = re.compile(
    r"\b(GROUP\s+BY|WINDOW|OVER\s*\(|CUBE|ROLLUP|GROUPING\s+SETS|INTERSECT|EXCEPT"
    r"|FULL\s+OUTER\s+JOIN|CROSS\s+JOIN|LATERAL)\b",
    re.IGNORECASE,
)

# Patterns that indicate local lightweight work
LOCAL_WORK_PATTERNS = re.compile(
    r"\b(read_csv|read_parquet|read_json|read_csv_auto|glob|COPY\s+|EXPORT\s+|DESCRIBE\s+|SHOW\s+|SET\s+|PRAGMA)\b",
    re.IGNORECASE,
)


class Engine(str, Enum):
    DUCKDB = "duckdb"          # Lightweight local engine
    STARBURST = "starburst"    # Distributed compute engine
    HYBRID = "hybrid"          # DuckDB orchestrates, Starburst computes parts


class JoinStrategy(str, Enum):
    NONE = "none"
    LOCAL_ONLY = "local_only"           # All data local to DuckDB
    REMOTE_ONLY = "remote_only"         # Full pushdown to Starburst
    HYBRID_PULL = "hybrid_pull"         # Starburst resolves remote, DuckDB joins locally
    STARBURST_OFFLOAD = "starburst_offload"  # DuckDB offloads entire query to Starburst


class RoutingReason(str, Enum):
    OWNED_NAMESPACE = "owned_namespace"
    LOCAL_TABLE = "local_table"
    LOCAL_FILE = "local_file"           # CSV/Parquet/Arrow file on local disk
    S3_OWNED_BUCKET = "s3_owned_bucket"
    SHARED_FILE = "shared_file"
    DEV_MODE = "dev_mode"
    BELOW_SCAN_THRESHOLD = "below_scan_threshold"
    CROSS_TEAM_REF = "cross_team_ref"
    SIZE_THRESHOLD = "size_threshold"
    HEAVY_COMPUTE = "heavy_compute"     # Complex analytics → offload to Starburst
    ROW_LEVEL_SECURITY = "row_level_security"
    EXPLICIT_HINT = "explicit_hint"
    CROSS_JOIN = "cross_join"
    DATA_PREP = "data_prep"             # Local file prep (CSV→Parquet, schema transform)
    STARBURST_PUSHDOWN = "starburst_pushdown"  # DuckDB pushes to Starburst directly


@dataclass
class ExecutionPlan:
    engine: Engine
    pushdown_sql: str | None = None      # SQL to send to Starburst
    local_sql: str | None = None         # SQL to execute in DuckDB
    join_strategy: JoinStrategy = JoinStrategy.NONE
    reasons: list[RoutingReason] = field(default_factory=list)
    snapshot_id: int | None = None
    as_of_timestamp: str | None = None
    tables_owned: list[str] = field(default_factory=list)
    tables_unowned: list[str] = field(default_factory=list)
    offload_to_starburst: bool = False   # When True, DuckDB relays to Starburst


@dataclass
class TableRef:
    catalog: str | None
    schema: str | None
    name: str
    full_name: str
    is_owned: bool = False
    is_s3_path: bool = False
    is_local: bool = False
    is_local_file: bool = False    # Local filesystem file (CSV, Parquet, etc.)
    is_function_call: bool = False


class QueryRouter:
    """Routes queries between DuckDB (lightweight client) and Starburst (distributed compute).

    The core principle: DuckDB handles lightweight local work (file I/O, data prep,
    small scans). Starburst handles heavy distributed compute (cross-team joins,
    large analytics, governed access). DuckDB can also relay queries to Starburst
    and cache/transform the results locally.
    """

    def __init__(
        self,
        owned_namespaces: set[str] | None = None,
        s3_buckets: list[str] | None = None,
        scan_threshold: int | None = None,
    ):
        self.owned_namespaces = owned_namespaces or settings.owned_namespace_set
        self.s3_buckets = s3_buckets or settings.s3_bucket_list
        self.scan_threshold = scan_threshold or settings.direct_scan_threshold

    def route(self, sql: str, *, estimated_bytes: int | None = None) -> ExecutionPlan:
        """Parse SQL and return an ExecutionPlan with routing decision.

        Decision hierarchy:
        1. Explicit hint → honor it
        2. Local file operations → always DuckDB (it's what DuckDB is for)
        3. Size threshold exceeded → Starburst distributed compute
        4. Heavy compute detected → Starburst (offload from DuckDB)
        5. Dev mode + owned tables → DuckDB direct
        6. Cross-team references → Starburst governed gateway
        7. Mixed owned/unowned → Hybrid (Starburst computes, DuckDB relays)
        8. All owned, small scan → DuckDB direct
        """
        # Extract hints before parsing (sqlglot doesn't preserve comments)
        engine_hint = self._extract_engine_hint(sql)
        snapshot_id = self._extract_snapshot_hint(sql)
        as_of_ts = self._extract_as_of_hint(sql)

        # Strip hints for parsing
        clean_sql = self._strip_hints(sql)

        # Parse table references
        table_refs = self._extract_table_refs(clean_sql)

        # Classify each table ref
        for ref in table_refs:
            ref.is_owned = self._is_owned(ref)
            ref.is_s3_path = self._is_s3_path(ref)
            ref.is_local = self._is_local(ref)
            ref.is_local_file = self._is_local_file(ref)

        owned = [r for r in table_refs if r.is_owned or r.is_s3_path or r.is_local]
        unowned = [r for r in table_refs if not (r.is_owned or r.is_s3_path or r.is_local)]

        plan = ExecutionPlan(
            engine=Engine.DUCKDB,
            local_sql=sql,
            snapshot_id=snapshot_id,
            as_of_timestamp=as_of_ts,
            tables_owned=[r.full_name for r in owned],
            tables_unowned=[r.full_name for r in unowned],
        )

        # ── Decision logic (ordered by priority) ─────────────────────────

        # 1. Explicit engine hint overrides everything
        if engine_hint == "starburst":
            plan.engine = Engine.STARBURST
            plan.pushdown_sql = sql
            plan.local_sql = None
            plan.offload_to_starburst = True
            plan.join_strategy = JoinStrategy.STARBURST_OFFLOAD
            plan.reasons.append(RoutingReason.EXPLICIT_HINT)
            self._emit_routing(plan)
            return plan

        if engine_hint == "duckdb":
            plan.engine = Engine.DUCKDB
            plan.local_sql = sql
            plan.reasons.append(RoutingReason.EXPLICIT_HINT)
            self._emit_routing(plan)
            return plan

        # 2. Local file operations → always DuckDB (its strength)
        if self._is_local_file_query(sql):
            plan.engine = Engine.DUCKDB
            plan.local_sql = sql
            plan.join_strategy = JoinStrategy.LOCAL_ONLY
            plan.reasons.append(RoutingReason.LOCAL_FILE)
            self._emit_routing(plan)
            return plan

        # 3. Data prep operations → always DuckDB
        if self._is_data_prep_query(sql):
            plan.engine = Engine.DUCKDB
            plan.local_sql = sql
            plan.join_strategy = JoinStrategy.LOCAL_ONLY
            plan.reasons.append(RoutingReason.DATA_PREP)
            self._emit_routing(plan)
            return plan

        # 4. Size threshold → offload to Starburst (too big for in-memory DuckDB)
        if estimated_bytes and estimated_bytes > self.scan_threshold:
            plan.engine = Engine.STARBURST
            plan.pushdown_sql = sql
            plan.local_sql = None
            plan.offload_to_starburst = True
            plan.join_strategy = JoinStrategy.STARBURST_OFFLOAD
            plan.reasons.append(RoutingReason.SIZE_THRESHOLD)
            self._emit_routing(plan)
            return plan

        # 5. Heavy compute on non-local data → offload to Starburst
        #    DuckDB is lightweight; Starburst has the distributed muscle
        if self._is_heavy_compute(sql) and not self._all_local_files(table_refs) and settings.starburst_enabled:
            plan.engine = Engine.STARBURST
            plan.pushdown_sql = sql
            plan.local_sql = None
            plan.offload_to_starburst = True
            plan.join_strategy = JoinStrategy.STARBURST_OFFLOAD
            plan.reasons.append(RoutingReason.HEAVY_COMPUTE)
            self._emit_routing(plan)
            return plan

        # 6. Dev mode: DuckDB for owned tables (fast iteration)
        if settings.is_dev and not unowned:
            plan.engine = Engine.DUCKDB
            plan.local_sql = sql
            plan.reasons.append(RoutingReason.DEV_MODE)
            self._emit_routing(plan)
            return plan

        # 7. All tables unowned → Starburst gateway (governed, cross-team)
        if unowned and not owned:
            plan.engine = Engine.STARBURST
            plan.pushdown_sql = sql
            plan.local_sql = None
            plan.offload_to_starburst = True
            plan.join_strategy = JoinStrategy.REMOTE_ONLY
            plan.reasons.append(RoutingReason.CROSS_TEAM_REF)
            self._emit_routing(plan)
            return plan

        # 8. Mix of owned and unowned → Hybrid
        #    Starburst resolves unowned side, DuckDB joins with local data
        if owned and unowned:
            plan.engine = Engine.HYBRID
            plan.join_strategy = JoinStrategy.HYBRID_PULL
            plan.reasons.append(RoutingReason.CROSS_JOIN)
            plan.pushdown_sql = self._build_unowned_subquery(clean_sql, unowned)
            plan.local_sql = sql
            self._emit_routing(plan)
            return plan

        # 9. All owned → DuckDB direct (small, fast, no hop)
        if owned and not unowned:
            plan.engine = Engine.DUCKDB
            plan.local_sql = sql
            plan.join_strategy = JoinStrategy.LOCAL_ONLY
            reasons = set()
            for ref in owned:
                if ref.is_local_file:
                    reasons.add(RoutingReason.LOCAL_FILE)
                elif ref.is_local:
                    reasons.add(RoutingReason.LOCAL_TABLE)
                elif ref.is_s3_path:
                    reasons.add(RoutingReason.S3_OWNED_BUCKET)
                elif ref.is_owned:
                    reasons.add(RoutingReason.OWNED_NAMESPACE)
            plan.reasons.extend(reasons)
            self._emit_routing(plan)
            return plan

        # 10. No table refs (DDL, SET, PRAGMA, etc.) → DuckDB
        if not table_refs:
            plan.engine = Engine.DUCKDB
            plan.local_sql = sql
            plan.reasons.append(RoutingReason.LOCAL_TABLE)
            self._emit_routing(plan)
            return plan

        # Default fallback: DuckDB
        plan.engine = Engine.DUCKDB
        plan.local_sql = sql
        plan.reasons.append(RoutingReason.OWNED_NAMESPACE)
        self._emit_routing(plan)
        return plan

    def _extract_table_refs(self, sql: str) -> list[TableRef]:
        """Extract all table references from SQL using sqlglot AST."""
        refs: list[TableRef] = []
        try:
            parsed = sqlglot.parse(sql, error_level=sqlglot.ErrorLevel.WARN)
        except Exception as exc:
            logger.warning("Failed to parse SQL: %s", exc)
            return refs

        for statement in parsed:
            if statement is None:
                continue
            for table in statement.find_all(exp.Table):
                catalog = table.catalog or None
                schema = table.db or None
                name = table.name

                if not name:
                    continue

                is_func = isinstance(table.parent, exp.TableAlias) and isinstance(
                    table.parent.parent, exp.Subquery
                )

                parts = [p for p in [catalog, schema, name] if p]
                full_name = ".".join(parts)

                refs.append(
                    TableRef(
                        catalog=catalog,
                        schema=schema,
                        name=name,
                        full_name=full_name,
                        is_function_call=is_func,
                    )
                )
        return refs

    def _is_owned(self, ref: TableRef) -> bool:
        """Check if a table ref belongs to an owned namespace."""
        if ref.schema and ref.schema in self.owned_namespaces:
            return True
        if ref.catalog and ref.catalog in self.owned_namespaces:
            return True
        if not ref.schema and not ref.catalog:
            return True
        return False

    def _is_s3_path(self, ref: TableRef) -> bool:
        """Check if this references a team-owned S3 bucket."""
        full = ref.full_name.lower()
        for bucket in self.s3_buckets:
            if bucket.lower() in full:
                return True
        return False

    def _is_local(self, ref: TableRef) -> bool:
        """Check if this is a local DuckDB table/view/temp table."""
        if not ref.schema and not ref.catalog:
            return True
        if ref.schema in ("main", "temp", "information_schema"):
            return True
        return False

    def _is_local_file(self, ref: TableRef) -> bool:
        """Check if this references a local filesystem file."""
        name = ref.name.lower()
        return name.endswith((".csv", ".parquet", ".json", ".arrow", ".xlsx"))

    def _is_local_file_query(self, sql: str) -> bool:
        """Check if the query is reading local files (DuckDB's strength)."""
        return bool(LOCAL_WORK_PATTERNS.search(sql))

    def _is_data_prep_query(self, sql: str) -> bool:
        """Check if this is a data preparation query (COPY, EXPORT, CREATE TABLE AS)."""
        upper = sql.strip().upper()
        return upper.startswith(("COPY ", "EXPORT ", "CREATE TABLE", "INSERT INTO"))

    def _is_heavy_compute(self, sql: str) -> bool:
        """Detect heavy analytical patterns that should be offloaded to Starburst.

        DuckDB is an in-memory lightweight engine — when we detect complex
        aggregations, window functions, or multi-way joins, we push to
        Starburst's distributed compute cluster instead.
        """
        return bool(HEAVY_COMPUTE_PATTERNS.search(sql))

    def _all_local_files(self, refs: list[TableRef]) -> bool:
        """Check if all table refs are local files."""
        return all(r.is_local_file or r.is_local for r in refs)

    def _build_unowned_subquery(self, sql: str, unowned: list[TableRef]) -> str:
        """Build the SQL fragment that Starburst needs to resolve for unowned tables."""
        return sql

    def _extract_engine_hint(self, sql: str) -> str | None:
        m = ENGINE_HINT_RE.search(sql)
        return m.group(1).lower() if m else None

    def _extract_snapshot_hint(self, sql: str) -> int | None:
        m = SNAPSHOT_HINT_RE.search(sql)
        return int(m.group(1)) if m else None

    def _extract_as_of_hint(self, sql: str) -> str | None:
        m = AS_OF_HINT_RE.search(sql)
        return m.group(1) if m else None

    def _strip_hints(self, sql: str) -> str:
        sql = ENGINE_HINT_RE.sub("", sql)
        sql = SNAPSHOT_HINT_RE.sub("", sql)
        sql = AS_OF_HINT_RE.sub("", sql)
        return sql.strip()

    def _emit_routing(self, plan: ExecutionPlan) -> None:
        """Log routing decision and emit Prometheus metric."""
        reason_str = ",".join(r.value for r in plan.reasons) if plan.reasons else "unknown"
        ROUTING_DECISIONS.labels(path=plan.engine.value, reason=reason_str).inc()
        logger.info(
            "Routed to %s: reasons=%s owned=%s unowned=%s offload=%s",
            plan.engine.value,
            reason_str,
            plan.tables_owned,
            plan.tables_unowned,
            plan.offload_to_starburst,
        )
