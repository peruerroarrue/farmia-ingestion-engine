"""
conftest.py
-----------
Fixtures compartidas entre todos los tests.
"""

import os
import json
import shutil
import pytest
from pathlib import Path
from pyspark.sql import SparkSession


# ---------------------------------------------------------------------------
# SparkSession local compartida entre todos los tests
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    """
    SparkSession local con Delta. Se crea una vez por sesión de tests
    y se reutiliza en todos los tests para no pagar el coste de arranque
    múltiples veces.
    """
    os.environ.setdefault("SPARK_LOCAL_DIRS", "/tmp/spark_tests")
    Path("/tmp/spark_tests").mkdir(parents=True, exist_ok=True)

    try:
        from delta import configure_spark_with_delta_pip
        builder = (
            SparkSession.builder
            .master("local[2]")
            .appName("farmia-tests")
            .config("spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.default.parallelism", "2")
        )
        session = configure_spark_with_delta_pip(builder).getOrCreate()
    except ImportError:
        session = (
            SparkSession.builder
            .master("local[2]")
            .appName("farmia-tests")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )

    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# Directorios temporales de prueba
# ---------------------------------------------------------------------------

@pytest.fixture()
def tmp_paths(tmp_path):
    """
    Crea la estructura de directorios landing/raw/bronze en una carpeta
    temporal que se limpia automáticamente tras cada test.
    """
    landing = tmp_path / "landing"
    raw = tmp_path / "raw"
    bronze = tmp_path / "bronze"
    landing.mkdir()
    raw.mkdir()
    bronze.mkdir()
    return {
        "landing": str(landing),
        "raw": str(raw),
        "bronze": str(bronze),
        "base": str(tmp_path),
    }


# ---------------------------------------------------------------------------
# Ficheros de datos sintéticos
# ---------------------------------------------------------------------------

@pytest.fixture()
def json_files(tmp_paths):
    """Crea dos ficheros JSON en landing para ecommerce/sales_orders."""
    path = Path(tmp_paths["landing"]) / "ecommerce" / "sales_orders"
    path.mkdir(parents=True)

    records_1 = [
        {"order_id": 1001, "customer_id": 1, "total_amount": 25.50,
         "status": "confirmed", "ordered_at": "2025-01-15T10:30:00"},
        {"order_id": 1002, "customer_id": 2, "total_amount": 10.00,
         "status": "pending",   "ordered_at": "2025-01-16T08:00:00"},
    ]
    records_2 = [
        {"order_id": 1003, "customer_id": 3, "total_amount": 55.75,
         "status": "shipped",   "ordered_at": "2025-01-17T14:20:00"},
    ]

    with open(path / "sales_001.json", "w") as f:
        for r in records_1:
            f.write(json.dumps(r) + "\n")

    with open(path / "sales_002.json", "w") as f:
        for r in records_2:
            f.write(json.dumps(r) + "\n")

    return str(path)


@pytest.fixture()
def csv_files(tmp_paths):
    """Crea un fichero CSV en landing para inventory/stock."""
    path = Path(tmp_paths["landing"]) / "inventory" / "stock"
    path.mkdir(parents=True)

    with open(path / "stock_001.csv", "w") as f:
        f.write("product_id,product_name,quantity,unit_cost,updated_at\n")
        f.write("1,Tomates Cherry,500,0.80,2025-01-15T10:00:00\n")
        f.write("2,Lechuga Romana,200,1.20,2025-01-15T10:00:00\n")
        f.write("3,Pimientos Rojos,150,2.50,2025-01-15T10:00:00\n")

    return str(path)


@pytest.fixture()
def parquet_files(spark, tmp_paths):
    """Crea un fichero Parquet en landing para logistics/shipments."""
    path = Path(tmp_paths["landing"]) / "logistics" / "shipments"
    path.mkdir(parents=True)

    data = [
        (1, 1001, "MRW",  "TRK001", "delivered", 2),
        (2, 1002, "SEUR", "TRK002", "in_transit", 3),
        (3, 1003, "DHL",  "TRK003", "pending",    1),
    ]
    df = spark.createDataFrame(
        data,
        "shipment_id long, order_id long, carrier string, "
        "tracking string, status string, estimated_days int"
    )
    df.coalesce(1).write.mode("overwrite").parquet(str(path))
    return str(path)


@pytest.fixture()
def image_files(tmp_paths):
    """Crea imágenes JPG sintéticas en landing para field_ops/crop_images."""
    path = Path(tmp_paths["landing"]) / "field_ops" / "crop_images"
    path.mkdir(parents=True)

    # JPEG mínimo válido
    jpg_bytes = bytes([
        0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10,
        0x4A, 0x46, 0x49, 0x46, 0x00,
        0x01, 0x01, 0x00,
        0x00, 0x01, 0x00, 0x01,
        0x00, 0x00,
        0xFF, 0xD9,
    ])
    for i in range(3):
        (path / f"crop_{i:03d}.jpg").write_bytes(jpg_bytes)

    return str(path)
