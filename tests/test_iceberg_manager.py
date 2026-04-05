"""Test IcebergManager: write, schema evolution, HMS registration."""

import os

os.environ.setdefault("DLS_MODE", "dev")

import pytest
import pyarrow as pa

from dlsidecar.iceberg.manager import IcebergManager, IcebergSchemaConflictError


class TestIcebergManager:
    def test_arrow_to_iceberg_type_int(self):
        mgr = IcebergManager()
        from pyiceberg.types import IntegerType
        result = mgr._arrow_to_iceberg_type(pa.int32())
        assert isinstance(result, IntegerType)

    def test_arrow_to_iceberg_type_long(self):
        mgr = IcebergManager()
        from pyiceberg.types import LongType
        result = mgr._arrow_to_iceberg_type(pa.int64())
        assert isinstance(result, LongType)

    def test_arrow_to_iceberg_type_string(self):
        mgr = IcebergManager()
        from pyiceberg.types import StringType
        result = mgr._arrow_to_iceberg_type(pa.string())
        assert isinstance(result, StringType)

    def test_arrow_to_iceberg_type_float(self):
        mgr = IcebergManager()
        from pyiceberg.types import FloatType
        result = mgr._arrow_to_iceberg_type(pa.float32())
        assert isinstance(result, FloatType)

    def test_arrow_to_iceberg_type_double(self):
        mgr = IcebergManager()
        from pyiceberg.types import DoubleType
        result = mgr._arrow_to_iceberg_type(pa.float64())
        assert isinstance(result, DoubleType)

    def test_arrow_to_iceberg_type_bool(self):
        mgr = IcebergManager()
        from pyiceberg.types import BooleanType
        result = mgr._arrow_to_iceberg_type(pa.bool_())
        assert isinstance(result, BooleanType)

    def test_arrow_to_iceberg_type_timestamp(self):
        mgr = IcebergManager()
        from pyiceberg.types import TimestampType
        result = mgr._arrow_to_iceberg_type(pa.timestamp("us"))
        assert isinstance(result, TimestampType)

    def test_safe_type_change(self):
        mgr = IcebergManager()
        from pyiceberg.types import IntegerType
        assert mgr._is_safe_type_change(IntegerType(), pa.int64()) is True

    def test_manager_init(self):
        mgr = IcebergManager()
        assert mgr._catalog is None
        assert mgr._hms_source is None
