# Databricks notebook source
# MAGIC %md
# MAGIC # FarmIA Kafka Producer
# MAGIC
# MAGIC Publica mensajes sintéticos en los topics de Kafka:
# MAGIC - `sensor_readings` — datos de sensores IoT en JSON
# MAGIC - `customer_events` — eventos de clientes en JSON

# COMMAND ----------

%pip install confluent-kafka httpx
dbutils.library.restartPython()

# COMMAND ----------

import sys
REPO_ROOT = "/Workspace/Repos/peruerro@ucm.es/farmia-ingestion-engine"
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga de credenciales desde Databricks Secrets

# COMMAND ----------

import os

SECRET_SCOPE = "farmia"

os.environ["KAFKA_SASL_USERNAME"]      = dbutils.secrets.get(SECRET_SCOPE, "kafka_sasl_username")
os.environ["KAFKA_SASL_PASSWORD"]      = dbutils.secrets.get(SECRET_SCOPE, "kafka_sasl_password")
os.environ["SCHEMA_REGISTRY_USERNAME"] = dbutils.secrets.get(SECRET_SCOPE, "schema_registry_username")
os.environ["SCHEMA_REGISTRY_PASSWORD"] = dbutils.secrets.get(SECRET_SCOPE, "schema_registry_password")

print(f"✅ Credenciales cargadas desde el scope '{SECRET_SCOPE}'")

# COMMAND ----------

from src.environment import load_config

CONFIG_PATH = f"{REPO_ROOT}/configs/datasets.yml"
env, _ = load_config(CONFIG_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuración del productor Kafka

# COMMAND ----------

from confluent_kafka import Producer
import json
import random
from datetime import datetime, timedelta

producer_conf = {
    "bootstrap.servers": env.kafka_bootstrap_servers,
    "security.protocol": env.kafka_security_protocol,
    "sasl.mechanism": env.kafka_sasl_mechanism,
    "sasl.username": env.kafka_sasl_username,
    "sasl.password": env.kafka_sasl_password,
}

producer = Producer(producer_conf)

def delivery_report(err, msg):
    if err:
        print(f"❌ Error al entregar mensaje: {err}")
    else:
        print(f"✅ Mensaje entregado a {msg.topic()} [{msg.partition()}] offset {msg.offset()}")

print("✅ Productor Kafka configurado")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Datos sintéticos

# COMMAND ----------

random.seed(42)

FIELD_ZONES   = ["Norte", "Sur", "Este", "Oeste", "Central"]
EVENT_TYPES   = ["page_view", "add_to_cart", "purchase", "search", "login"]
PLATFORMS     = ["ios", "android", "web"]
PAGES         = ["home", "product", "cart", "checkout", "search"]

def random_ts(days_back: int = 1) -> str:
    dt = datetime.now() - timedelta(
        hours=random.randint(0, days_back * 24),
        minutes=random.randint(0, 59),
    )
    return dt.isoformat()

def make_sensor_reading(i: int) -> dict:
    return {
        "sensor_id":    f"SNS-{random.randint(1, 50):03d}",
        "field_zone":   random.choice(FIELD_ZONES),
        "temperature":  round(random.uniform(5.0, 45.0), 2),
        "humidity":     round(random.uniform(10.0, 95.0), 2),
        "soil_quality": round(random.uniform(0.0, 10.0), 2),
        "battery_level":round(random.uniform(0.0, 100.0), 1),
        "read_at":      random_ts(),
    }

def make_customer_event(i: int) -> dict:
    return {
        "event_id":    f"EVT-{i:06d}",
        "customer_id": random.randint(1, 500),
        "event_type":  random.choice(EVENT_TYPES),
        "platform":    random.choice(PLATFORMS),
        "session_id":  f"SES-{random.randint(1000, 9999)}",
        "page_url":    f"https://farmia.es/{random.choice(PAGES)}",
        "product_id":  random.randint(1, 50) if random.random() > 0.4 else None,
        "duration_ms": random.randint(100, 30000),
        "occurred_at": random_ts(),
    }

# COMMAND ----------
# MAGIC %md
# MAGIC ## Publicación de mensajes

# COMMAND ----------

N_MESSAGES = 50

print(f"📤 Publicando {N_MESSAGES} mensajes en sensor_readings...")
for i in range(1, N_MESSAGES + 1):
    msg = make_sensor_reading(i)
    producer.produce(
        topic="sensor_readings",
        key=msg["sensor_id"],
        value=json.dumps(msg),
        callback=delivery_report,
    )
    if i % 10 == 0:
        producer.poll(0)

producer.flush()
print(f"✅ {N_MESSAGES} mensajes publicados en sensor_readings")

# COMMAND ----------

print(f"📤 Publicando {N_MESSAGES} mensajes en customer_events...")
for i in range(1, N_MESSAGES + 1):
    msg = make_customer_event(i)
    producer.produce(
        topic="customer_events",
        key=msg["event_id"],
        value=json.dumps(msg),
        callback=delivery_report,
    )
    if i % 10 == 0:
        producer.poll(0)

producer.flush()
print(f"✅ {N_MESSAGES} mensajes publicados en customer_events")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verificación en Confluent Cloud
# MAGIC
# MAGIC Ve a Confluent Cloud → Topics → sensor_readings / customer_events
# MAGIC → pestaña Messages para ver los mensajes publicados.
