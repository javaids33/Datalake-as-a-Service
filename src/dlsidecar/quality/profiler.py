"""Data quality profiler — basic column statistics."""

from __future__ import annotations

from typing import Any

import pyarrow as pa


def profile_table(table: pa.Table) -> dict[str, Any]:
    """Generate basic column statistics for an Arrow table."""
    profile = {"row_count": len(table), "columns": {}}
    for col_name in table.column_names:
        col = table.column(col_name)
        stats: dict[str, Any] = {
            "type": str(col.type),
            "null_count": col.null_count,
            "null_pct": round(col.null_count / max(len(table), 1) * 100, 2),
        }
        if pa.types.is_integer(col.type) or pa.types.is_floating(col.type):
            import pyarrow.compute as pc
            stats["min"] = pc.min(col).as_py()
            stats["max"] = pc.max(col).as_py()
            stats["mean"] = pc.mean(col).as_py()
        elif pa.types.is_string(col.type) or pa.types.is_large_string(col.type):
            import pyarrow.compute as pc
            stats["unique_count"] = pc.count_distinct(col).as_py()
        profile["columns"][col_name] = stats
    return profile
