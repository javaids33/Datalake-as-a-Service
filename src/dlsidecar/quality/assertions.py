"""Data quality assertions — simple check framework."""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pyarrow.compute as pc


class AssertionError(Exception):
    pass


def assert_not_null(table: pa.Table, columns: list[str]) -> None:
    for col in columns:
        if col in table.column_names:
            null_count = table.column(col).null_count
            if null_count > 0:
                raise AssertionError(f"Column '{col}' has {null_count} null values")


def assert_unique(table: pa.Table, columns: list[str]) -> None:
    for col in columns:
        if col in table.column_names:
            total = len(table)
            unique = pc.count_distinct(table.column(col)).as_py()
            if unique < total:
                raise AssertionError(f"Column '{col}' has {total - unique} duplicate values")


def assert_row_count(table: pa.Table, min_rows: int = 0, max_rows: int | None = None) -> None:
    count = len(table)
    if count < min_rows:
        raise AssertionError(f"Expected at least {min_rows} rows, got {count}")
    if max_rows is not None and count > max_rows:
        raise AssertionError(f"Expected at most {max_rows} rows, got {count}")
