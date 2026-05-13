# FarmIA Ingestion Engine

Motor de ingesta de datos para FarmIA, una startup agrícola en expansión nacional. Implementa una arquitectura **Data Lakehouse** sobre Azure Databricks con ingesta batch y streaming.

---

## Parte 1 — Arquitectura del Data Lakehouse

### Diagrama

```
Fuentes de datos
────────────────────────────────────────────────────────────
  Ventas online (JSON)     ──┐
  Inventario (CSV)         ──┤
  Logística (Parquet)      ──┤──► LANDING ──► BRONZE ──► SILVER ──► GOLD
  Imágenes campo (JPG)     ──┤
  Sensores IoT (Kafka JSON)──┤
  Eventos cliente (Kafka)  ──┘
```

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      AZURE DATABRICKS LAKEHOUSE                         │
│                                                                         │
│  ┌──────────┐      ┌──────────┐      ┌──────────┐      ┌──────────┐   │
│  │ LANDING  │─────►│  BRONZE  │─────►│  SILVER  │─────►│   GOLD   │   │
│  │          │      │          │      │          │      │          │   │
│  │ Ficheros │      │ Delta    │      │ Delta    │      │ Delta    │   │
│  │ crudos   │      │ +metadat │      │ limpia   │      │ agregada │   │
│  │ (origen) │      │ inmutable│      │ +negocio │      │ +KPIs    │   │
│  └──────────┘      └──────────┘      └──────────┘      └──────────┘   │
│                                                                         │
│  /Volumes/workspace/default/                                            │
│  └── landing/      └── bronze/       (futuras capas Silver / Gold)     │
└─────────────────────────────────────────────────────────────────────────┘
```

### Descripción de cada capa

#### Landing
Zona de aterrizaje de los datos en crudo. Los sistemas origen depositan aquí los ficheros sin ninguna transformación. Es la única capa a la que escriben los sistemas externos y actúa también como **archivo inmutable** del dato original — no se modifica ni se elimina, lo que permite reprocesar Bronze ante cualquier bug en la ingesta.

- **Formato:** nativo del origen (JSON, CSV, Parquet, JPG, mensajes Kafka)
- **Acceso:** solo escritura desde sistemas origen, solo lectura desde el motor
- **Retención:** permanente (auditoría y reprocesamiento)
- **Ejemplos FarmIA:**
  - `landing/ecommerce/sales_orders/sales_orders_001.json`
  - `landing/inventory/stock/stock_001.csv`
  - `landing/field_ops/crop_images/maduros_001.jpg`

#### Bronze
Primera capa del lakehouse en formato Delta Lake. Contiene los datos tal como llegaron pero enriquecidos con metadatos de ingesta y convertidos a un formato unificado y consultable.

- **Formato:** Delta Lake (Parquet + transaction log)
- **Transformaciones:** solo metadatos — `_ingested_at`, `_datasource`, `_dataset`, `_ingested_filename`
- **Schema evolution:** habilitado para absorber cambios compatibles en origen
- **Retención:** permanente
- **Ejemplos FarmIA:**
  - `bronze/ecommerce/sales_orders/` — pedidos con metadatos de ingesta
  - `bronze/iot/sensor_readings/` — lecturas de sensores particionadas por `field_zone`
  - `bronze/mobile/customer_events/` — eventos de clientes particionados por `event_type`

#### Silver *(capa futura)*
Datos limpios, validados y enriquecidos. Aquí se aplican las reglas de negocio, deduplicación y joins entre datasets.

- **Transformaciones propuestas para FarmIA:**
  - `silver.ventas` — pedidos con datos de cliente y producto enriquecidos
  - `silver.inventario` — stock normalizado con alertas de mínimos
  - `silver.sensores` — lecturas de sensores con anomalías detectadas
  - `silver.logistica` — envíos con tiempos de entrega calculados

#### Gold *(capa futura)*
Agregaciones y métricas de negocio listas para consumo por dashboards y modelos ML.

- **Tablas propuestas para FarmIA:**
  - `gold.ventas_diarias_por_zona` — revenue por zona geográfica
  - `gold.alertas_campo` — sensores con valores fuera de rango
  - `gold.prediccion_cosecha` — modelo ML de predicción basado en sensores + meteorología
  - `gold.kpis_logistica` — tiempos de entrega por transportista y zona

---

## Parte 2 — Motor de Ingesta

### Arquitectura del código

```
farmia-ingestion-engine/
├── src/
│   ├── config.py        # Dataclasses de configuración (Environment, DatasetConfig...)
│   ├── environment.py   # Carga del YAML + resolución de ${VAR} desde el entorno
│   ├── reader.py        # BatchReader y StreamingReader
│   ├── writer.py        # BronzeWriter (escribe en Delta)
│   └── engine.py        # IngestionEngine (orquestador principal)
├── configs/
│   └── datasets.yml     # Configuración de los 6 datasets de FarmIA
├── notebooks/
│   ├── 01_generate_datasets.py  # Genera datos sintéticos en Landing
│   ├── 02_run_engine.py         # Ejecuta el motor de ingesta
│   ├── 03_kafka_producer.py     # Publica mensajes en topics Kafka
│   └── 04_run_tests.py          # Ejecuta tests de integración
├── tests/
│   ├── conftest.py          # Fixtures compartidas (SparkSession, datos de prueba)
│   ├── test_config.py       # Tests unitarios de config y environment (21 tests)
│   └── test_batch_reader.py # Tests de integración de BatchReader (9 tests)
├── pyproject.toml
└── README.md
```

### Datasets configurados

| # | Datasource | Dataset | Formato | Tipo | Autoloader |
|---|-----------|---------|---------|------|-----------|
| 1 | ecommerce | sales_orders | JSON | Batch | ✅ |
| 2 | inventory | stock | CSV | Batch | ✅ |
| 3 | logistics | shipments | Parquet | Batch | ✅ |
| 4 | field_ops | crop_images | BinaryFile (JPG) | Batch | — |
| 5 | iot | sensor_readings | Kafka JSON | Streaming | n/a |
| 6 | mobile | customer_events | Kafka JSON | Streaming | n/a |

### Flujo de ingesta

```
Batch:
  Landing (ficheros) ──► BatchReader (cloudFiles) ──► BronzeWriter ──► Bronze (Delta)

Streaming:
  Kafka topic        ──► StreamingReader          ──► BronzeWriter ──► Bronze (Delta)
```

El mismo `BronzeWriter` sirve para batch y streaming: un único patrón
`writeStream.format("delta").trigger(availableNow=True).start(bronze_path)`.
La capa Landing actúa como archivo inmutable de los ficheros originales —
no se modifican ni se mueven tras procesarse, lo que mantiene la trazabilidad
y permite reprocesar Bronze en cualquier momento.

### Gestión de credenciales

Las credenciales sensibles (Kafka, Schema Registry) **no viven en el repo**.
`configs/datasets.yml` solo contiene placeholders `${KAFKA_SASL_PASSWORD}`, etc.
`environment.py` los resuelve contra `os.environ` al cargar el YAML —
si la variable no existe lanza `EnvironmentError` con mensaje claro.

En Databricks, los notebooks (`02_run_engine.py` y `03_kafka_producer.py`)
inyectan los valores desde un secret scope (`dbutils.secrets.get → os.environ`)
**antes** de llamar a `load_config(...)`. En local se exportan como variables de
entorno normales. El motor no conoce el origen — solo ve env vars resueltas.

---

## Requisitos previos

### Entorno Databricks
- Databricks Free Edition con Unity Catalog habilitado
- Volúmenes creados en Unity Catalog:
  ```sql
  CREATE VOLUME IF NOT EXISTS workspace.default.landing;
  CREATE VOLUME IF NOT EXISTS workspace.default.bronze;
  ```

### Kafka (para datasets streaming)
- Cluster en Confluent Cloud (Free Trial)
- Topics creados: `sensor_readings`, `customer_events`
- Credenciales gestionadas mediante un Databricks **secret scope** (ver siguiente sección)

### Configuración del secret scope

Las credenciales **no se commitean al repo** — viven en un secret scope de Databricks
y se inyectan como variables de entorno antes de cargar el YAML. El YAML
solo contiene placeholders `${KAFKA_SASL_PASSWORD}`, etc.

Hay dos formas equivalentes de crear el scope. Elige la que prefieras.

**Opción A — Con la CLI de Databricks** (requiere `databricks` instalado y autenticado):

```bash
databricks secrets create-scope farmia
databricks secrets put-secret farmia kafka_sasl_username      --string-value "TU_API_KEY"
databricks secrets put-secret farmia kafka_sasl_password      --string-value "TU_API_SECRET"
databricks secrets put-secret farmia schema_registry_username --string-value "TU_SR_KEY"
databricks secrets put-secret farmia schema_registry_password --string-value "TU_SR_SECRET"
```

**Opción B — Desde un notebook one-shot** (sin instalar nada; aprovecha el contexto autenticado del propio workspace). Crea un notebook `00_setup_secrets` fuera del repo y bórralo después:

```python
import requests
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host  = ctx.apiUrl().get()
token = ctx.apiToken().get()
headers = {"Authorization": f"Bearer {token}"}

# Crear el scope (devuelve 400 si ya existe — ignorar)
requests.post(f"{host}/api/2.0/secrets/scopes/create",
              headers=headers, json={"scope": "farmia"})

secrets = {
    "kafka_sasl_username":      "TU_API_KEY",
    "kafka_sasl_password":      "TU_API_SECRET",
    "schema_registry_username": "TU_SR_KEY",
    "schema_registry_password": "TU_SR_SECRET",
}
for key, value in secrets.items():
    requests.post(f"{host}/api/2.0/secrets/put", headers=headers,
                  json={"scope": "farmia", "key": key, "string_value": value})
```

Para ejecutar **en local**, exporta las mismas variables como variables de entorno
(o usa un `.env` con `python-dotenv`):

```powershell
$env:KAFKA_SASL_USERNAME      = "TU_API_KEY"
$env:KAFKA_SASL_PASSWORD      = "TU_API_SECRET"
$env:SCHEMA_REGISTRY_USERNAME = "TU_SR_KEY"
$env:SCHEMA_REGISTRY_PASSWORD = "TU_SR_SECRET"
```

### Entorno local (para tests)
- Python 3.11
- Java 17 (OpenJDK Temurin recomendado)
- `SPARK_HOME` **no debe estar definido** (causa conflictos con PySpark)

```bash
pip install pyspark==3.5.1 delta-spark==3.2.0 pyyaml pytest pytest-timeout
```

---

## Ejecución

### 1. Configurar el entorno

Edita `configs/datasets.yml` con tus rutas y URLs (las credenciales **no van aquí**,
se referencian con placeholders `${VAR}` que se resuelven desde el secret scope):

```yaml
environment:
  landing_path: "/Volumes/workspace/default/landing"
  bronze_path: "/Volumes/workspace/default/bronze"
  kafka_bootstrap_servers: "tu-cluster.confluent.cloud:9092"
  kafka_sasl_username: "${KAFKA_SASL_USERNAME}"
  kafka_sasl_password: "${KAFKA_SASL_PASSWORD}"
  # ... resto de credenciales como placeholders
```

Si aún no creaste el secret scope, ve a la sección **Configuración del secret scope** más arriba.

### 2. Generar datos de prueba (Databricks)

Abre y ejecuta `notebooks/01_generate_datasets.py`. Genera 200 registros por dataset en la capa Landing.

### 3. Publicar mensajes Kafka (Databricks)

Abre y ejecuta `notebooks/03_kafka_producer.py`. Publica 50 mensajes en cada topic.

### 4. Ejecutar el motor de ingesta (Databricks)

Abre y ejecuta `notebooks/02_run_engine.py`. El motor procesará los 6 datasets y escribirá en Bronze.

Salida esperada:
```
🚀 Iniciando motor de ingesta — 6 datasets
▶️  Query lanzada: ecommerce/sales_orders
▶️  Query lanzada: inventory/stock
▶️  Query lanzada: logistics/shipments
▶️  Query lanzada: field_ops/crop_images
▶️  Query lanzada: iot/sensor_readings
▶️  Query lanzada: mobile/customer_events
✅ Completada: ecommerce/sales_orders
✅ Completada: inventory/stock
✅ Completada: logistics/shipments
✅ Completada: field_ops/crop_images
✅ Completada: iot/sensor_readings
✅ Completada: mobile/customer_events
📊 Resumen: 6/6 ingestas completadas
```

### 5. Ejecutar tests (local)

```bash
# Asegúrate de que SPARK_HOME no está definido y Java 17 está activo
pytest tests/ -v --timeout=120
```

Salida esperada: **30 passed**

---

## Notas sobre el entorno (Databricks Free Edition Serverless)

El motor está diseñado para funcionar tanto en Databricks Serverless Free Edition como en cluster clásico, sin cambios de código. Adaptaciones aplicadas:

- **`input_file_name()` no soportado en Unity Catalog** → se usa `_metadata.file_path`, que es la API oficial recomendada por Databricks (mismo rendimiento).
- **DBFS root deshabilitado** → se usan rutas de Unity Catalog Volumes (`/Volumes/...`). En Azure productivo bastaría con cambiar las rutas a `abfss://...` en el YAML; el motor no requiere cambios.
- **Avro como streaming source restringido en clusters compartidos** → los topics Kafka usan JSON. El código de decodificación Avro con Schema Registry sigue implementado en `StreamingReader._decode_avro()` y se activa cambiando `value_format: avro` en el YAML cuando se ejecute en un cluster dedicado.
- **Avro batch sí funciona** — `mobile/customer_events` se genera en Avro en Landing y `BatchReader` lo procesa correctamente.

### Tests
Los tests unitarios (`test_config.py`, 21 tests) y de integración con Spark (`test_batch_reader.py`, 9 tests) se ejecutan en local. Los tests de streaming (Kafka) y escritura Delta se validan mediante la ejecución end-to-end del motor en Databricks.

### Troubleshooting

**Kafka: `SaslAuthenticationException — Authentication failed. If you are using a Global API key...`**

Pese a lo que dice el mensaje, no es necesariamente que tu key sea Global.
Habitualmente está causado por uno de estos tres motivos:

1. **La API key se creó en el sitio equivocado.** Tiene que ser una **Cluster API key**:
   Confluent Cloud → Environments → tu environment → Cluster `farmia-cluster` → pestaña **API keys** → **+ Add key**.
   Si la generaste en *Account & Access → API keys* (nivel cuenta) no servirá para el broker.
2. **El secret guardado en el scope tiene caracteres residuales** (un `\n` al final por copy-paste). Verifica `len(dbutils.secrets.get(...))` — el secret de Confluent debe ser de 64 caracteres exactos.
3. **La key arrastra estado raro** (especialmente si rotaste varias veces). Bórrala desde Confluent y crea una nueva — fix definitivo en muchos casos.

Diagnóstico rápido sin Spark:

```python
from confluent_kafka.admin import AdminClient
admin = AdminClient({
    "bootstrap.servers": env.kafka_bootstrap_servers,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism":    "PLAIN",
    "sasl.username":     dbutils.secrets.get("farmia", "kafka_sasl_username"),
    "sasl.password":     dbutils.secrets.get("farmia", "kafka_sasl_password"),
})
print(list(admin.list_topics(timeout=10).topics.keys()))
```

Si esto lista los topics, el problema está aguas abajo (Spark, ACLs). Si falla con auth, está en la key/scope.

**Streaming: `Cannot resume query because checkpoint location is in invalid state`**

Si cambiaste el reader de un dataset (p. ej. activaste/desactivaste Autoloader) sobre un Bronze ya escrito,
el checkpoint queda incompatible. Bórralo y relanza:

```python
dbutils.fs.rm("/Volumes/workspace/default/bronze/<datasource>/<dataset>_checkpoint", True)
```

### Empaquetado como librería (mejora futura)

En un entorno productivo, el motor se empaquetaría como una librería Python (wheel) para instalarse directamente en el cluster de Databricks:

```bash
pip install build
python -m build
```

```python
%pip install /path/to/farmia_ingestion_engine-1.0.0-py3-none-any.whl

from farmia_ingestion_engine import IngestionEngine
from farmia_ingestion_engine.environment import load_config
```

La estructura actual del proyecto (`src/` layout con `pyproject.toml`) ya está preparada para este paso — solo requeriría añadir la configuración de build en `pyproject.toml`.
