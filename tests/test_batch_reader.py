"""
test_batch_reader.py
--------------------
Tests para BatchReader con SparkSession local.
Verifica que cada formato se lee correctamente y que los metadatos
se añaden a cada registro.
"""

import pytest
from src.config import Environment, DatasetConfig, BatchSourceConfig
from src.reader import BatchReader

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Fixture de entorno
# ---------------------------------------------------------------------------

@pytest.fixture()
def env(tmp_paths):
    return Environment(
        landing_path=tmp_paths["landing"],
        raw_path=tmp_paths["raw"],
        bronze_path=tmp_paths["bronze"],
    )


# ---------------------------------------------------------------------------
# Tests formato JSON
# ---------------------------------------------------------------------------

class TestBatchReaderJson:

    def test_reads_json_files(self, spark, env, json_files):
        dataset = DatasetConfig(
            datasource="ecommerce",
            dataset="sales_orders",
            source=BatchSourceConfig(
                format="json",
                use_autoloader=False,
            ),
        )
        reader = BatchReader(spark, env)
        df = reader.read(dataset)
        batch = df._jdf  # fuerza la creación del plan sin ejecutar
        assert df is not None

    def test_json_adds_metadata_columns(self, spark, env, json_files):
        dataset = DatasetConfig(
            datasource="ecommerce",
            dataset="sales_orders",
            source=BatchSourceConfig(
                format="json",
                use_autoloader=False,
            ),
        )
        reader = BatchReader(spark, env)
        df = reader.read(dataset)
        cols = df.columns
        assert "_ingested_at" in cols
        assert "_ingested_filename" in cols
        assert "_datasource" in cols
        assert "_dataset" in cols
        assert "ingestion_date" in cols

    def test_json_datasource_value(self, spark, env, json_files):
        dataset = DatasetConfig(
            datasource="ecommerce",
            dataset="sales_orders",
            source=BatchSourceConfig(
                format="json",
                use_autoloader=False,
            ),
        )
        reader = BatchReader(spark, env)
        df = reader.read(dataset)

        # Ejecutamos el stream con trigger once para verificar los datos
        import tempfile, os
        checkpoint = tempfile.mkdtemp()
        output = tempfile.mkdtemp()

        query = (
            df.writeStream
            .format("parquet")
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .start(output)
        )
        query.awaitTermination(timeout=30)

        result = spark.read.parquet(output)
        datasources = [r["_datasource"] for r in result.select("_datasource").collect()]
        assert all(d == "ecommerce" for d in datasources)
        assert result.count() == 3  # 2 + 1 registros de los dos ficheros


# ---------------------------------------------------------------------------
# Tests formato CSV
# ---------------------------------------------------------------------------

class TestBatchReaderCsv:

    def test_reads_csv_files(self, spark, env, csv_files):
        dataset = DatasetConfig(
            datasource="inventory",
            dataset="stock",
            source=BatchSourceConfig(
                format="csv",
                use_autoloader=False,
                options={"header": "true"},
            ),
        )
        reader = BatchReader(spark, env)
        df = reader.read(dataset)
        assert df is not None
        assert "_ingested_at" in df.columns

    def test_csv_record_count(self, spark, env, csv_files):
        dataset = DatasetConfig(
            datasource="inventory",
            dataset="stock",
            source=BatchSourceConfig(
                format="csv",
                use_autoloader=False,
                options={"header": "true"},
            ),
        )
        reader = BatchReader(spark, env)
        df = reader.read(dataset)

        import tempfile
        checkpoint = tempfile.mkdtemp()
        output = tempfile.mkdtemp()

        query = (
            df.writeStream
            .format("parquet")
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .start(output)
        )
        query.awaitTermination(timeout=30)

        result = spark.read.parquet(output)
        assert result.count() == 3


# ---------------------------------------------------------------------------
# Tests formato Parquet
# ---------------------------------------------------------------------------

class TestBatchReaderParquet:

    def test_reads_parquet_files(self, spark, env, parquet_files):
        dataset = DatasetConfig(
            datasource="logistics",
            dataset="shipments",
            source=BatchSourceConfig(
                format="parquet",
                use_autoloader=False,
            ),
        )
        reader = BatchReader(spark, env)
        df = reader.read(dataset)
        assert df is not None
        assert "_ingested_at" in df.columns

    def test_parquet_record_count(self, spark, env, parquet_files):
        dataset = DatasetConfig(
            datasource="logistics",
            dataset="shipments",
            source=BatchSourceConfig(
                format="parquet",
                use_autoloader=False,
            ),
        )
        reader = BatchReader(spark, env)
        df = reader.read(dataset)

        import tempfile
        checkpoint = tempfile.mkdtemp()
        output = tempfile.mkdtemp()

        query = (
            df.writeStream
            .format("parquet")
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .start(output)
        )
        query.awaitTermination(timeout=30)
        result = spark.read.parquet(output)
        assert result.count() == 3


# ---------------------------------------------------------------------------
# Tests formato BinaryFile (imágenes)
# ---------------------------------------------------------------------------

class TestBatchReaderImages:

    def test_reads_image_files(self, spark, env, image_files):
        dataset = DatasetConfig(
            datasource="field_ops",
            dataset="crop_images",
            source=BatchSourceConfig(
                format="binaryFile",
                use_autoloader=False,
                options={
                    "pathGlobFilter": "*.jpg",
                    "recursiveFileLookup": "true",
                },
            ),
        )
        reader = BatchReader(spark, env)
        df = reader.read(dataset)
        assert df is not None
        assert "_ingested_at" in df.columns
        # binaryFile no tiene _ingested_filename, tiene 'path' nativo
        assert "_ingested_filename" not in df.columns
        assert "path" in df.columns

    def test_image_count(self, spark, env, image_files):
        dataset = DatasetConfig(
            datasource="field_ops",
            dataset="crop_images",
            source=BatchSourceConfig(
                format="binaryFile",
                use_autoloader=False,
                options={
                    "pathGlobFilter": "*.jpg",
                    "recursiveFileLookup": "true",
                },
            ),
        )
        reader = BatchReader(spark, env)
        df = reader.read(dataset)

        import tempfile
        checkpoint = tempfile.mkdtemp()
        output = tempfile.mkdtemp()

        query = (
            df.writeStream
            .format("parquet")
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .start(output)
        )
        query.awaitTermination(timeout=30)
        result = spark.read.parquet(output)
        assert result.count() == 3
