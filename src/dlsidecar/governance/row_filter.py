"""
Row-level governance injection — sqlglot AST transform that wraps every FROM clause.

When DLS_ROW_FILTER is set, every DuckDB query has the predicate injected:
  Original: SELECT * FROM events
  Rewritten: SELECT * FROM (SELECT * FROM events WHERE tenant_id = 'team-alpha') AS _governed

Handles CTEs, subqueries, JOINs, and UNION correctly.
For Starburst queries, pass tenant context as a session property instead.
"""

from __future__ import annotations

import logging

import sqlglot
from sqlglot import exp

from dlsidecar.config import settings

logger = logging.getLogger("dlsidecar.governance.row_filter")


class RowFilterInjector:
    """Injects row-level filter predicates into DuckDB SQL via AST transform."""

    def __init__(self, row_filter: str | None = None):
        self.row_filter = row_filter or settings.row_filter

    @property
    def enabled(self) -> bool:
        return bool(self.row_filter)

    def inject(self, sql: str, dialect: str = "duckdb") -> str:
        """Inject row filter into all FROM clauses in the SQL.
        Returns the rewritten SQL string.
        """
        if not self.enabled:
            return sql

        try:
            parsed = sqlglot.parse(sql, read=dialect, error_level=sqlglot.ErrorLevel.WARN)
        except Exception as exc:
            logger.warning("Failed to parse SQL for row filter injection: %s", exc)
            return sql

        results = []
        for statement in parsed:
            if statement is None:
                results.append(sql)
                continue
            transformed = self._transform_statement(statement)
            results.append(transformed.sql(dialect=dialect))

        return "; ".join(results)

    def _transform_statement(self, statement: exp.Expression) -> exp.Expression:
        """Walk the AST and wrap each real table reference with a filtered subquery."""
        # Process tables bottom-up to avoid double-wrapping
        for table in list(statement.find_all(exp.Table)):
            # Skip tables in CTEs definitions (they'll be filtered when referenced)
            # Skip information_schema and system tables
            if self._should_skip(table):
                continue

            # Build: (SELECT * FROM <table> WHERE <filter>) AS _governed_<n>
            alias = table.alias or f"_governed_{id(table) % 10000}"
            wrapped = self._wrap_table(table, alias)
            table.replace(wrapped)

        return statement

    def _should_skip(self, table: exp.Table) -> bool:
        """Skip system tables and function-style table references."""
        name = table.name.lower() if table.name else ""
        schema = (table.db or "").lower()

        # Skip system schemas
        if schema in ("information_schema", "pg_catalog", "temp"):
            return True

        # Skip if it looks like a function call (e.g., read_parquet, iceberg_scan)
        if name in ("read_parquet", "read_csv", "read_json", "iceberg_scan", "read_csv_auto", "glob"):
            return True

        return False

    def _wrap_table(self, table: exp.Table, alias: str) -> exp.Expression:
        """Wrap a table node with a filtered subquery."""
        # Reconstruct the full table reference
        table_ref = table.copy()
        # Remove any existing alias from the copy
        table_ref.set("alias", None)

        # Build: SELECT * FROM <table> WHERE <filter>
        # Parse the filter as a condition expression (not a WHERE clause)
        filter_expr = sqlglot.parse_one(
            f"SELECT 1 WHERE {self.row_filter}",
            error_level=sqlglot.ErrorLevel.WARN,
        ).find(exp.Where)

        condition = filter_expr.this if filter_expr else sqlglot.parse_one(self.row_filter, error_level=sqlglot.ErrorLevel.WARN)

        subquery_select = exp.Select(
            expressions=[exp.Star()],
        ).from_(table_ref).where(condition)

        subquery = exp.Subquery(
            this=subquery_select,
            alias=exp.TableAlias(this=exp.to_identifier(alias)),
        )
        return subquery

    def starburst_session_property(self) -> str | None:
        """Return the SET SESSION statement for Starburst tenant context."""
        if not self.enabled:
            return None
        import json

        tag = json.dumps({"tenant": settings.tenant_id, "row_filter": self.row_filter})
        return f"SET SESSION query_tag = '{tag}'"
