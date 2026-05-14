"""
test_engine.py
--------------
Tests del orquestador IngestionEngine.

Por ahora solo cubre las clases auxiliares (IngestionResult, IngestionError)
sin necesidad de SparkSession. Los tests end-to-end del motor se ejecutan
en Databricks vía el notebook 02_run_engine.py.
"""

import pytest
from src.engine import IngestionResult, IngestionError


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
