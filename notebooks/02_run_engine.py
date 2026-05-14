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
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Intalación de confluent-kafka

# COMMAND ----------

# MAGIC %pip install confluent-kafka
# MAGIC %pip install confluent-kafka httpx
# MAGIC

# COMMAND ----------

dbutils.library.restartPython()

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
# MAGIC ## 2. Carga de credenciales desde Databricks Secrets
# MAGIC
# MAGIC Las credenciales de Kafka y Schema Registry viven en un secret scope
# MAGIC y se inyectan como variables de entorno antes de cargar el YAML.
# MAGIC Ver README → "Configuración del secret scope" para crearlo.

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
# MAGIC ## 3. Carga de configuración

# COMMAND ----------

from src.environment import load_config

CONFIG_PATH = f"{REPO_ROOT}/configs/datasets.yml"
env, datasets = load_config(CONFIG_PATH)

print(f"\nEntorno:")
print(f"  landing : {env.landing_path}")
print(f"  bronze  : {env.bronze_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Ejecución del motor

# COMMAND ----------

from src.engine import IngestionEngine

# Configurar catálogo por defecto del workspace
spark.sql("USE CATALOG workspace")
spark.sql("USE SCHEMA default")

# En Databricks la SparkSession ya existe — la inyectamos al motor
# para que no intente crear una nueva
engine = IngestionEngine(env=env, datasets=datasets, spark=spark)
engine.run()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verificación — contenido de bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Ventas online (JSON → Delta)

# COMMAND ----------

df_sales = spark.read.format("delta").load(
    f"{env.bronze_path}/ecommerce/sales_orders"
)
print(f"Registros ingestados: {df_sales.count()}")
display(df_sales.orderBy("_ingested_at").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Inventario (CSV → Delta)

# COMMAND ----------

df_stock = spark.read.format("delta").load(
    f"{env.bronze_path}/inventory/stock"
)
print(f"Registros ingestados: {df_stock.count()}")
display(df_stock.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Logística (Parquet → Delta)

# COMMAND ----------

df_shipments = spark.read.format("delta").load(
    f"{env.bronze_path}/logistics/shipments"
)
print(f"Registros ingestados: {df_shipments.count()}")
display(df_shipments.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.4 Imágenes de campo (BinaryFile → Delta)

# COMMAND ----------

df_images = spark.read.format("delta").load(
    f"{env.bronze_path}/field_ops/crop_images"
)
print(f"Imágenes ingestadas: {df_images.count()}")
display(df_images.select("path", "_ingested_at").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.5 Sensores IoT (JSON → Delta)

# COMMAND ----------

df_sensors = spark.read.format("delta").load(
    f"{env.bronze_path}/iot/sensor_readings"
)
print(f"Registros ingestados: {df_sensors.count()}")
display(df_sensors.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.6 Eventos de clientes (Avro → Delta)

# COMMAND ----------

df_events = spark.read.format("delta").load(
    f"{env.bronze_path}/mobile/customer_events"
)
print(f"Registros ingestados: {df_events.count()}")
display(df_events.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Resumen de metadatos de ingesta

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Tablas registradas en Unity Catalog
# MAGIC
# MAGIC El motor registra cada dataset como tabla externa en
# MAGIC `{bronze_catalog}.{bronze_schema}.{datasource}__{dataset}`.
# MAGIC A partir de aquí las tablas son consultables desde el SQL Editor.

# COMMAND ----------

if env.bronze_catalog and env.bronze_schema:
    display(spark.sql(
        f"SHOW TABLES IN {env.bronze_catalog}.{env.bronze_schema}"
    ))
else:
    print("ℹ️  bronze_catalog/bronze_schema no configurados — sin registro UC")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejemplo de consulta SQL sobre Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _datasource, COUNT(*) AS total_registros, MAX(_ingested_at) AS ultima_ingesta
# MAGIC FROM workspace.bronze.ecommerce__sales_orders
# MAGIC GROUP BY _datasource
