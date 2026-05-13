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
│   ├── environment.py   # Carga del YAML y construcción de objetos
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
│   ├── test_config.py       # Tests unitarios de config y environment (19 tests)
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
- Credenciales configuradas en `configs/datasets.yml`

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

Edita `configs/datasets.yml` con tus credenciales:

```yaml
environment:
  landing_path: "/Volumes/workspace/default/landing"
  bronze_path: "/Volumes/workspace/default/bronze"
  kafka_bootstrap_servers: "tu-cluster.confluent.cloud:9092"
  kafka_sasl_username: "tu_api_key"
  kafka_sasl_password: "tu_api_secret"
  # ... resto de credenciales
```

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

Salida esperada: **28 passed**

---

## Notas sobre el entorno (Databricks Free Edition Serverless)

El motor está diseñado para funcionar tanto en Databricks Serverless Free Edition como en cluster clásico, sin cambios de código. Adaptaciones aplicadas:

- **`input_file_name()` no soportado en Unity Catalog** → se usa `_metadata.file_path`, que es la API oficial recomendada por Databricks (mismo rendimiento).
- **DBFS root deshabilitado** → se usan rutas de Unity Catalog Volumes (`/Volumes/...`). En Azure productivo bastaría con cambiar las rutas a `abfss://...` en el YAML; el motor no requiere cambios.
- **Avro como streaming source restringido en clusters compartidos** → los topics Kafka usan JSON. El código de decodificación Avro con Schema Registry sigue implementado en `StreamingReader._decode_avro()` y se activa cambiando `value_format: avro` en el YAML cuando se ejecute en un cluster dedicado.
- **Avro batch sí funciona** — `mobile/customer_events` se genera en Avro en Landing y `BatchReader` lo procesa correctamente.

### Tests
Los tests unitarios (`test_config.py`, 19 tests) y de integración con Spark (`test_batch_reader.py`, 9 tests) se ejecutan en local. Los tests de streaming (Kafka) y escritura Delta se validan mediante la ejecución end-to-end del motor en Databricks.

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
