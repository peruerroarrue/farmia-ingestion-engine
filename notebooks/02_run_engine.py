# Databricks notebook source
# MAGIC %md
# MAGIC # FarmIA Ingestion Engine
# MAGIC
# MAGIC Notebook de ejecución del motor de ingesta.
# MAGIC Lee la configuración de `configs/datasets.yml` e instancia el motor
# MAGIC que procesa todos los datasets configurados.
# MAGIC
# MAGIC ## Requisitos previos
# MAGIC 1. Haber ejecutado `01_generate_datasets.py` para generar los datos en landing.
# MAGIC 2. Tener el cluster serverless activo.
# MAGIC
# MAGIC ## Capas involucradas
# MAGIC ```
# MAGIC Landing  →  (motor lee)  →  Bronze (Delta)
# MAGIC                         →  Raw    (archivo inmutable)
# MAGIC ```

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Setup — rutas e imports

# COMMAND ----------

import sys
import logging

# Añadimos la raíz del repo al path para que los imports de src funcionen
REPO_ROOT = "/Workspace/Repos/peruerro@ucm.es/farmia-ingestion-engine"
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)

# Configurar catálogo por defecto del workspace  ← AÑADE ESTAS LÍNEAS
spark.catalog.setCurrentCatalog("workspace")
spark.sql("USE SCHEMA default")

print(f"✅ Repo root añadido al path: {REPO_ROOT}")
print(f"✅ Spark version: {spark.version}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Carga de configuración

# COMMAND ----------

from src.environment import load_config

CONFIG_PATH = f"{REPO_ROOT}/configs/datasets.yml"
env, datasets = load_config(CONFIG_PATH)

print(f"\nEntorno:")
print(f"  landing : {env.landing_path}")
print(f"  raw     : {env.raw_path}")
print(f"  bronze  : {env.bronze_path}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Ejecución del motor

# COMMAND ----------

from src.engine import IngestionEngine

# Configurar catálogo por defecto del workspace
spark.conf.set("spark.databricks.unity.catalog.enabled", "true")
spark.sql("USE CATALOG workspace")
spark.sql("USE SCHEMA default")

# En Databricks la SparkSession ya existe — la inyectamos al motor
# para que no intente crear una nueva
engine = IngestionEngine(env=env, datasets=datasets, spark=spark)
engine.run()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Verificación — contenido de bronze

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4.1 Ventas online (JSON → Delta)

# COMMAND ----------

df_sales = spark.read.format("delta").load(
    f"{env.bronze_path}/ecommerce/sales_orders"
)
print(f"Registros ingestados: {df_sales.count()}")
display(df_sales.orderBy("_ingested_at").limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4.2 Inventario (CSV → Delta)

# COMMAND ----------

df_stock = spark.read.format("delta").load(
    f"{env.bronze_path}/inventory/stock"
)
print(f"Registros ingestados: {df_stock.count()}")
display(df_stock.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4.3 Logística (Parquet → Delta)

# COMMAND ----------

df_shipments = spark.read.format("delta").load(
    f"{env.bronze_path}/logistics/shipments"
)
print(f"Registros ingestados: {df_shipments.count()}")
display(df_shipments.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4.4 Imágenes de campo (BinaryFile → Delta)

# COMMAND ----------

df_images = spark.read.format("delta").load(
    f"{env.bronze_path}/field_ops/crop_images"
)
print(f"Imágenes ingestadas: {df_images.count()}")
display(df_images.select("path", "_ingested_at").limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4.5 Sensores IoT (JSON → Delta)

# COMMAND ----------

df_sensors = spark.read.format("delta").load(
    f"{env.bronze_path}/iot/sensor_readings"
)
print(f"Registros ingestados: {df_sensors.count()}")
display(df_sensors.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4.6 Eventos de clientes (Avro → Delta)

# COMMAND ----------

df_events = spark.read.format("delta").load(
    f"{env.bronze_path}/mobile/customer_events"
)
print(f"Registros ingestados: {df_events.count()}")
display(df_events.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Resumen de metadatos de ingesta

# COMMAND ----------

import pyspark.sql.functions as F

datasets_info = [
    ("ecommerce/sales_orders", df_sales),
    ("inventory/stock",        df_stock),
    ("logistics/shipments",    df_shipments),
    ("field_ops/crop_images",  df_images),
    ("iot/sensor_readings",    df_sensors),
    ("mobile/customer_events", df_events),
]

print(f"\n{'Dataset':<30} {'Registros':>10} {'Última ingesta'}")
print("─" * 70)
for name, df in datasets_info:
    count = df.count()
    last = df.agg(F.max("_ingested_at")).collect()[0][0]
    print(f"{name:<30} {count:>10} {last}")
