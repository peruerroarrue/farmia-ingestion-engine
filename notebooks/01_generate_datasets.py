# Databricks notebook source
# Si ejecutas en local, comenta la línea de SparkSession y usa la que
# crea el motor automáticamente. En Databricks 'spark' ya existe.

# COMMAND ----------

# MAGIC %md
# MAGIC # Generador de Datasets Sintéticos — FarmIA
# MAGIC
# MAGIC Genera datos de prueba para los 6 datasets configurados y los
# MAGIC deposita en la capa landing con la estructura esperada por el motor.
# MAGIC
# MAGIC | # | Dataset              | Formato     | Tipo     |
# MAGIC |---|----------------------|-------------|----------|
# MAGIC | 1 | ecommerce/sales_orders | JSON      | Batch    |
# MAGIC | 2 | inventory/stock        | CSV       | Batch    |
# MAGIC | 3 | logistics/shipments    | Parquet   | Batch    |
# MAGIC | 4 | field_ops/crop_images  | BinaryFile| Batch    |
# MAGIC | 5 | iot/sensor_readings    | Kafka JSON| Streaming|
# MAGIC | 6 | mobile/customer_events | Kafka Avro| Streaming|

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.default;
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.default.landing;
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.default.raw;
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.default.bronze;

# COMMAND ----------

import os
from pathlib import Path
import json
import random
import struct
from datetime import datetime, timedelta


from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, IntegerType, DoubleType,
    TimestampType, BinaryType, BooleanType,
)
import pyspark.sql.functions as F

# ------------------------------------------------------------------
# SparkSession — en Databricks ya existe como 'spark'
# ------------------------------------------------------------------
try:
    spark  # noqa: F821 — en Databricks ya está definida
except NameError:
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder
        .master("local[*]")
        .appName("FarmIA Dataset Generator")
        .config("spark.local.dir", "C:/tmp/spark")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

# ------------------------------------------------------------------
# Rutas base — ajusta según tu entorno
# En Databricks Free Edition: "dbfs:/farmia/landing"
# En local:                   "/tmp/farmia/landing"

LANDING_BASE = "/Volumes/workspace/default/landing"
N_RECORDS = 200  # registros por dataset

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utilidades

# COMMAND ----------

random.seed(42)

def landing_path(datasource: str, dataset: str) -> str:
    path = f"{LANDING_BASE}/{datasource}/{dataset}"
    Path(path).mkdir(parents=True, exist_ok=True)
    return path

PRODUCT_NAMES = [
    "Tomates Cherry", "Lechuga Romana", "Pimientos Rojos",
    "Zanahorias Baby", "Pepinos", "Espinacas", "Cebollas",
    "Ajos", "Patatas", "Berenjenas",
]
FIELD_ZONES = ["Norte", "Sur", "Este", "Oeste", "Central"]
EVENT_TYPES = ["page_view", "add_to_cart", "purchase", "search", "login"]
CARRIERS = ["MRW", "SEUR", "DHL", "Correos Express", "GLS"]
SHIPMENT_STATUSES = ["pending", "in_transit", "delivered", "returned"]

def random_timestamp(days_back: int = 30) -> datetime:
    return datetime.now() - timedelta(
        days=random.randint(0, days_back),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )

def ts(days_back: int = 30) -> str:
    return random_timestamp(days_back).isoformat()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Ventas online — JSON

# COMMAND ----------

sales_records = []
for i in range(1, N_RECORDS + 1):
    n_items = random.randint(1, 5)
    items = [
        {
            "product_id": random.randint(1, 50),
            "product_name": random.choice(PRODUCT_NAMES),
            "quantity": random.randint(1, 10),
            "unit_price": round(random.uniform(0.5, 15.0), 2),
        }
        for _ in range(n_items)
    ]
    total = sum(it["quantity"] * it["unit_price"] for it in items)
    sales_records.append({
        "order_id": 1000 + i,
        "customer_id": random.randint(1, 500),
        "customer_email": f"cliente{random.randint(1,500)}@farmia.es",
        "items": items,
        "total_amount": round(total, 2),
        "currency": "EUR",
        "status": random.choice(["pending", "confirmed", "shipped", "delivered"]),
        "ordered_at": ts(),
        "shipped_at": ts(20) if random.random() > 0.3 else None,
        "is_first_order": random.choice([True, False]),
    })

out = landing_path("ecommerce", "sales_orders")
with open(f"{out}/sales_orders_001.json", "w", encoding="utf-8") as f:
    for rec in sales_records[:100]:
        f.write(json.dumps(rec) + "\n")
with open(f"{out}/sales_orders_002.json", "w", encoding="utf-8") as f:
    for rec in sales_records[100:]:
        f.write(json.dumps(rec) + "\n")

print(f"✅ ecommerce/sales_orders — {N_RECORDS} registros en {out}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Inventario — CSV

# COMMAND ----------

stock_schema = StructType([
    StructField("product_id",    LongType(),      False),
    StructField("product_name",  StringType(),    False),
    StructField("category",      StringType(),    False),
    StructField("quantity",      IntegerType(),   False),
    StructField("unit_cost",     DoubleType(),    False),
    StructField("warehouse_id",  IntegerType(),   False),
    StructField("updated_at",    TimestampType(), False),
    StructField("is_perishable", BooleanType(),   False),
])

stock_rows = [
    (
        i,
        random.choice(PRODUCT_NAMES),
        random.choice(["Verduras", "Frutas", "Legumbres", "Cereales"]),
        random.randint(0, 5000),
        round(random.uniform(0.10, 8.00), 2),
        random.randint(1, 5),
        random_timestamp(),
        random.choice([True, False]),
    )
    for i in range(1, N_RECORDS + 1)
]

stock_df = spark.createDataFrame(stock_rows, schema=stock_schema)
out = landing_path("inventory", "stock")
stock_df.coalesce(1).write.mode("overwrite").option("header", True).csv(out)
print(f"✅ inventory/stock — {N_RECORDS} registros en {out}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Logística / Envíos — Parquet

# COMMAND ----------

shipments_schema = StructType([
    StructField("shipment_id",     LongType(),      False),
    StructField("order_id",        LongType(),      False),
    StructField("carrier",         StringType(),    False),
    StructField("tracking_number", StringType(),    False),
    StructField("origin_zip",      StringType(),    False),
    StructField("destination_zip", StringType(),    False),
    StructField("weight_kg",       DoubleType(),    False),
    StructField("status",          StringType(),    False),
    StructField("estimated_days",  IntegerType(),   False),
    StructField("shipped_at",      TimestampType(), True),
    StructField("delivered_at",    TimestampType(), True),
])

shipments_rows = [
    (
        i,
        random.randint(1000, 1999),
        random.choice(CARRIERS),
        f"TRK{random.randint(100000, 999999)}",
        f"{random.randint(10000, 52999):05d}",
        f"{random.randint(10000, 52999):05d}",
        round(random.uniform(0.1, 30.0), 2),
        random.choice(SHIPMENT_STATUSES),
        random.randint(1, 7),
        random_timestamp(30),
        random_timestamp(10) if random.random() > 0.4 else None,
    )
    for i in range(1, N_RECORDS + 1)
]

shipments_df = spark.createDataFrame(shipments_rows, schema=shipments_schema)
out = landing_path("logistics", "shipments")
shipments_df.coalesce(1).write.mode("overwrite").parquet(out)
print(f"✅ logistics/shipments — {N_RECORDS} registros en {out}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Imágenes de campo — BinaryFile (JPG sintético)

# COMMAND ----------

def make_minimal_jpg(width: int = 8, height: int = 8) -> bytes:
    """
    Genera un JPEG mínimo válido con píxeles de color aleatorio.
    Usa la estructura SOI + APP0 + SOF0 + SOS + EOI simplificada.
    No requiere PIL — es un JPEG sintético para pruebas de pipeline.
    """
    r, g, b = random.randint(0, 255), random.randint(0, 255), random.randint(0, 255)
    # JFIF header mínimo
    header = bytes([
        0xFF, 0xD8,             # SOI
        0xFF, 0xE0,             # APP0 marker
        0x00, 0x10,             # length = 16
        0x4A, 0x46, 0x49, 0x46, 0x00,  # "JFIF\0"
        0x01, 0x01,             # version 1.1
        0x00,                   # aspect ratio units
        0x00, 0x01, 0x00, 0x01, # X/Y density
        0x00, 0x00,             # thumbnail size
    ])
    # Payload mínimo con color y EOI
    payload = bytes([r, g, b] * (width * height))
    eoi = bytes([0xFF, 0xD9])
    return header + payload + eoi

images_dir = landing_path("field_ops", "crop_images")
labels = ["maduros", "verdes", "danados", "optimos"]
n_images = 40

for i in range(1, n_images + 1):
    label = random.choice(labels)
    filename = f"{images_dir}/{label}_{i:03d}.jpg"
    with open(filename, "wb") as f:
        f.write(make_minimal_jpg())

print(f"✅ field_ops/crop_images — {n_images} imágenes JPG en {images_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 & 6. Datasets Streaming (Kafka)
# MAGIC
# MAGIC Los datasets de Kafka (sensor_readings y customer_events) se generan
# MAGIC desde un productor Kafka externo o mediante scripts de prueba.
# MAGIC
# MAGIC Para los **tests locales** del motor de ingesta, simulamos la entrada
# MAGIC Kafka escribiendo los datos en formato JSON/Avro en landing, de forma
# MAGIC que el BatchReader pueda procesarlos sin necesidad de un cluster Kafka.
# MAGIC
# MAGIC En **Databricks con Kafka real**, estos datasets se configuran como
# MAGIC streaming en datasets.yml y el motor los procesa automáticamente.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5a. Sensor readings — JSON (simulado en landing para tests)

# COMMAND ----------

sensor_records = []
for i in range(1, N_RECORDS + 1):
    sensor_records.append({
        "sensor_id": f"SNS-{random.randint(1, 50):03d}",
        "field_zone": random.choice(FIELD_ZONES),
        "temperature": round(random.uniform(5.0, 45.0), 2),
        "humidity": round(random.uniform(10.0, 95.0), 2),
        "soil_quality": round(random.uniform(0.0, 10.0), 2),
        "battery_level": round(random.uniform(0.0, 100.0), 1),
        "read_at": ts(),
    })

out = landing_path("iot", "sensor_readings")
with open(f"{out}/sensor_readings_001.json", "w", encoding="utf-8") as f:
    for rec in sensor_records:
        f.write(json.dumps(rec) + "\n")

print(f"✅ iot/sensor_readings — {N_RECORDS} registros JSON en {out}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5b. Customer events — Avro (simulado en landing para tests)

# COMMAND ----------

events_schema = StructType([
    StructField("event_id",    StringType(),    False),
    StructField("customer_id", LongType(),      False),
    StructField("event_type",  StringType(),    False),
    StructField("platform",    StringType(),    False),
    StructField("session_id",  StringType(),    False),
    StructField("page_url",    StringType(),    True),
    StructField("product_id",  LongType(),      True),
    StructField("duration_ms", IntegerType(),   True),
    StructField("occurred_at", TimestampType(), False),
])

events_rows = [
    (
        f"EVT-{i:06d}",
        random.randint(1, 500),
        random.choice(EVENT_TYPES),
        random.choice(["ios", "android", "web"]),
        f"SES-{random.randint(1000, 9999)}",
        f"https://farmia.es/{random.choice(['home', 'product', 'cart', 'checkout'])}",
        random.randint(1, 50) if random.random() > 0.4 else None,
        random.randint(100, 30000),
        random_timestamp(),
    )
    for i in range(1, N_RECORDS + 1)
]

events_df = spark.createDataFrame(events_rows, schema=events_schema)
out = landing_path("mobile", "customer_events")
events_df.coalesce(1).write.mode("overwrite").format("avro").save(out)
print(f"✅ mobile/customer_events — {N_RECORDS} registros Avro en {out}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen de ficheros generados

# COMMAND ----------

print("\n📁 Estructura de landing generada:")
for root, dirs, files in os.walk(LANDING_BASE):
    level = root.replace(LANDING_BASE, "").count(os.sep)
    indent = "   " * level
    folder = os.path.basename(root)
    print(f"{indent}📂 {folder}/")
    subindent = "   " * (level + 1)
    for f in files:
        size = os.path.getsize(os.path.join(root, f))
        print(f"{subindent}📄 {f}  ({size:,} bytes)")
