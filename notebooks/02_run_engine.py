# Databricks notebook source
# MAGIC %md
# MAGIC # FarmIA Ingestion Engine
# MAGIC
# MAGIC Notebook de ejecución del motor de ingesta. Procesa los 6 datasets
# MAGIC configurados en `configs/datasets.yml` y los escribe en Bronze como
# MAGIC **tablas managed de Unity Catalog**.
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
# MAGIC Pon `RESET = True` para empezar desde cero. Útil:
# MAGIC - **Primera ejecución** tras cambiar el sink (path → tabla UC managed)
# MAGIC - Tras fallos parciales en ejecuciones anteriores
# MAGIC - Para forzar un reprocesado completo desde Landing/Kafka
# MAGIC
# MAGIC Detiene queries activas, borra checkpoints y elimina el schema bronze.

# COMMAND ----------

RESET = False  # ⚠️ Ponlo a True solo cuando lo necesites

if RESET:
    for q in spark.streams.active:
        print(f"Deteniendo query activa: {q.name}")
        q.stop()
    dbutils.fs.rm("/Volumes/workspace/default/bronze", True)
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
print(f"  UC sink : {env.bronze_catalog}.{env.bronze_schema}")

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
# MAGIC ## 6. Tablas registradas en Unity Catalog
# MAGIC
# MAGIC Cada dataset queda como tabla managed bajo `workspace.bronze` y se
# MAGIC puede consultar directamente desde el SQL Editor.

# COMMAND ----------

display(spark.sql(
    f"SHOW TABLES IN {env.bronze_catalog}.{env.bronze_schema}"
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verificación — conteo y última ingesta por dataset

# COMMAND ----------

import pyspark.sql.functions as F

table_names = [
    "ecommerce__sales_orders",
    "inventory__stock",
    "logistics__shipments",
    "field_ops__crop_images",
    "iot__sensor_readings",
    "mobile__customer_events",
]

print(f"\n{'Tabla':<35} {'Registros':>10}   {'Última ingesta'}")
print("─" * 80)

for table in table_names:
    fqn = f"{env.bronze_catalog}.{env.bronze_schema}.{table}"
    try:
        df = spark.read.table(fqn)
        count = df.count()
        last = df.agg(F.max("_ingested_at")).collect()[0][0]
        print(f"{table:<35} {count:>10}   {last}")
    except Exception as e:
        print(f"{table:<35}        ❌   {str(e)[:60]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Ejemplos de queries SQL sobre Bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.1 Últimos pedidos ingestados

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, customer_id, total_amount, ordered_at, _ingested_at
# MAGIC FROM workspace.bronze.ecommerce__sales_orders
# MAGIC ORDER BY _ingested_at DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.2 Sensores IoT — temperatura media por zona

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   field_zone,
# MAGIC   COUNT(*) AS lecturas,
# MAGIC   ROUND(AVG(temperature), 2) AS temp_media,
# MAGIC   ROUND(AVG(humidity), 2) AS humedad_media
# MAGIC FROM workspace.bronze.iot__sensor_readings
# MAGIC GROUP BY field_zone
# MAGIC ORDER BY lecturas DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.3 Eventos de cliente — distribución por tipo y plataforma

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   event_type,
# MAGIC   platform,
# MAGIC   COUNT(*) AS eventos
# MAGIC FROM workspace.bronze.mobile__customer_events
# MAGIC GROUP BY event_type, platform
# MAGIC ORDER BY event_type, eventos DESC;
