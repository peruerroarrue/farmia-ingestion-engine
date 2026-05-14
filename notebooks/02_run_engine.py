# Databricks notebook source
# MAGIC %md
# MAGIC # FarmIA Ingestion Engine
# MAGIC
# MAGIC Notebook de ejecución del motor de ingesta. Procesa los 6 datasets
# MAGIC configurados en `configs/datasets.yml` y los escribe en Bronze como
# MAGIC tablas Delta.
# MAGIC
# MAGIC ## Requisitos previos
# MAGIC 1. Haber ejecutado `01_generate_datasets.py` (datos en Landing).
# MAGIC 2. Haber ejecutado `03_kafka_producer.py` (mensajes en Kafka).
# MAGIC 3. Tener el secret scope `farmia` creado con las 4 credenciales
# MAGIC    (ver README → "Configuración del secret scope").

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Instalación de dependencias

# COMMAND ----------

# MAGIC %pip install confluent-kafka
# MAGIC %pip install confluent-kafka httpx

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup — paths e imports

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

# Catálogo y schema por defecto del workspace
spark.catalog.setCurrentCatalog("workspace")
spark.sql("USE SCHEMA default")

print(f"✅ Repo root añadido al path: {REPO_ROOT}")
print(f"✅ Spark version: {spark.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Carga de credenciales desde Databricks Secrets
# MAGIC
# MAGIC Las credenciales de Kafka y Schema Registry viven en un secret scope
# MAGIC y se inyectan como variables de entorno antes de cargar el YAML.

# COMMAND ----------

import os

SECRET_SCOPE = "farmia"

os.environ["KAFKA_SASL_USERNAME"]      = dbutils.secrets.get(SECRET_SCOPE, "kafka_sasl_username")
os.environ["KAFKA_SASL_PASSWORD"]      = dbutils.secrets.get(SECRET_SCOPE, "kafka_sasl_password")
os.environ["SCHEMA_REGISTRY_USERNAME"] = dbutils.secrets.get(SECRET_SCOPE, "schema_registry_username")
os.environ["SCHEMA_REGISTRY_PASSWORD"] = dbutils.secrets.get(SECRET_SCOPE, "schema_registry_password")

print(f"✅ Credenciales cargadas desde el scope '{SECRET_SCOPE}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Limpieza (opcional)
# MAGIC
# MAGIC Pon `RESET = True` para empezar desde cero: detiene queries activas
# MAGIC y borra el contenido de Bronze (data + checkpoints + schemaLocation
# MAGIC de Autoloader). Útil tras fallos parciales en ejecuciones anteriores
# MAGIC o para forzar un reprocesado completo desde Landing/Kafka.

# COMMAND ----------

RESET = False  # ⚠️ Ponlo a True solo cuando necesites un reset completo

if RESET:
    for q in spark.streams.active:
        print(f"Deteniendo query activa: {q.name}")
        q.stop()
    dbutils.fs.rm("/Volumes/workspace/default/bronze", True)
    # Por si quedó residuo de algún experimento previo con tablas UC bronze
    spark.sql("DROP SCHEMA IF EXISTS workspace.bronze CASCADE")
    print("🧹 Bronze limpio — listo para empezar de cero")
else:
    print("ℹ️  RESET=False — ejecutando incremental sobre el Bronze existente")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Carga de configuración

# COMMAND ----------

from src.environment import load_config

CONFIG_PATH = f"{REPO_ROOT}/configs/datasets.yml"
env, datasets = load_config(CONFIG_PATH)

print(f"\nEntorno:")
print(f"  landing : {env.landing_path}")
print(f"  bronze  : {env.bronze_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Ejecución del motor
# MAGIC
# MAGIC `engine.run()` devuelve un `IngestionResult` con las listas de éxitos
# MAGIC y fallos. Si alguna ingesta falla, lanza `IngestionError` (lo que hace
# MAGIC que un Databricks Job acabe en estado FAILED). Para abortar al primer
# MAGIC error, instanciar con `fail_fast=True`.

# COMMAND ----------

from src.engine import IngestionEngine

engine = IngestionEngine(env=env, datasets=datasets, spark=spark)
result = engine.run()
result

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verificación — contenido de Bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 Ventas online (JSON → Delta)

# COMMAND ----------

df_sales = spark.read.format("delta").load(
    f"{env.bronze_path}/ecommerce/sales_orders"
)
print(f"Registros ingestados: {df_sales.count()}")
display(df_sales.orderBy("_ingested_at").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Inventario (CSV → Delta)

# COMMAND ----------

df_stock = spark.read.format("delta").load(
    f"{env.bronze_path}/inventory/stock"
)
print(f"Registros ingestados: {df_stock.count()}")
display(df_stock.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 Logística (Parquet → Delta)

# COMMAND ----------

df_shipments = spark.read.format("delta").load(
    f"{env.bronze_path}/logistics/shipments"
)
print(f"Registros ingestados: {df_shipments.count()}")
display(df_shipments.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.4 Imágenes de campo (BinaryFile → Delta)

# COMMAND ----------

df_images = spark.read.format("delta").load(
    f"{env.bronze_path}/field_ops/crop_images"
)
print(f"Imágenes ingestadas: {df_images.count()}")
display(df_images.select("path", "_ingested_at").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.5 Sensores IoT (JSON → Delta)

# COMMAND ----------

df_sensors = spark.read.format("delta").load(
    f"{env.bronze_path}/iot/sensor_readings"
)
print(f"Registros ingestados: {df_sensors.count()}")
display(df_sensors.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.6 Eventos de clientes (Kafka JSON → Delta)

# COMMAND ----------

df_events = spark.read.format("delta").load(
    f"{env.bronze_path}/mobile/customer_events"
)
print(f"Registros ingestados: {df_events.count()}")
display(df_events.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Resumen de metadatos de ingesta

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

print(f"\n{'Dataset':<30} {'Registros':>10}   {'Última ingesta'}")
print("─" * 75)
for name, df in datasets_info:
    count = df.count()
    last = df.agg(F.max("_ingested_at")).collect()[0][0]
    print(f"{name:<30} {count:>10}   {last}")
