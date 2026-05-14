"""
test_engine.py
--------------
Tests del orquestador IngestionEngine.

Por ahora solo cubre las clases auxiliares (IngestionResult, IngestionError)
sin necesidad de SparkSession. Los tests end-to-end del motor se ejecutan
en Databricks vía el notebook 02_run_engine.py.
"""

import pytest
from src.config import Environment, DatasetConfig, BatchSourceConfig
from src.engine import IngestionResult, IngestionError
from src.writer import BronzeWriter


# ---------------------------------------------------------------------------
# IngestionResult
# ---------------------------------------------------------------------------

class TestIngestionResult:

    def test_empty_result_is_all_ok(self):
        result = IngestionResult()
        assert result.total == 0
        assert result.all_ok is True
        assert result.succeeded == []
        assert result.failed == []

    def test_only_succeeded_is_all_ok(self):
        result = IngestionResult()
        result.succeeded.append("ecommerce/sales_orders")
        result.succeeded.append("inventory/stock")
        assert result.total == 2
        assert result.all_ok is True

    def test_with_failure_is_not_all_ok(self):
        result = IngestionResult()
        result.succeeded.append("ecommerce/sales_orders")
        result.failed.append(("inventory/stock", "boom"))
        assert result.total == 2
        assert result.all_ok is False

    def test_failed_keeps_error_message(self):
        result = IngestionResult()
        result.failed.append(("inventory/stock", "ConnectionError: timeout"))
        assert result.failed[0] == ("inventory/stock", "ConnectionError: timeout")


# ---------------------------------------------------------------------------
# IngestionError
# ---------------------------------------------------------------------------

class TestIngestionError:

    def test_is_runtime_error_subclass(self):
        # Importante: Databricks Jobs propaga RuntimeError como FAILED
        assert issubclass(IngestionError, RuntimeError)

    def test_can_be_raised_with_message(self):
        with pytest.raises(IngestionError, match="3/6 ingestas fallaron"):
            raise IngestionError("3/6 ingestas fallaron: ['a', 'b', 'c']")

    def test_chains_original_exception(self):
        original = ValueError("schema mismatch")
        try:
            try:
                raise original
            except ValueError as e:
                raise IngestionError("envuelta") from e
        except IngestionError as wrapped:
            assert wrapped.__cause__ is original


# ---------------------------------------------------------------------------
# BronzeWriter.register_table — comportamiento sin Spark real
# ---------------------------------------------------------------------------

class TestBronzeWriterRegisterTable:

    def _dataset(self) -> DatasetConfig:
        return DatasetConfig(
            datasource="ecommerce",
            dataset="sales_orders",
            source=BatchSourceConfig(format="json"),
        )

    def test_returns_none_when_catalog_not_set(self):
        env = Environment(landing_path="/l", bronze_path="/b")  # sin catalog/schema
        writer = BronzeWriter(spark=None, env=env)  # spark no se usa en este path
        assert writer.register_table(self._dataset()) is None

    def test_returns_none_when_only_catalog_set(self):
        env = Environment(landing_path="/l", bronze_path="/b", bronze_catalog="ws")
        writer = BronzeWriter(spark=None, env=env)
        assert writer.register_table(self._dataset()) is None

    def test_table_name_format(self):
        # Convención: {datasource}__{dataset}
        assert BronzeWriter._table_name(self._dataset()) == "ecommerce__sales_orders"
