from dataclasses import dataclass, field
from typing import Optional


# ---------------------------------------------------------------------------
# Environment — rutas base y credenciales del entorno (Databricks / local)
# ---------------------------------------------------------------------------

@dataclass
class Environment:
    """
    Contiene toda la información dependiente del entorno de ejecución.
    Se instancia una vez y se inyecta al motor, que nunca accede
    directamente a variables de entorno ni rutas hardcodeadas.
    """
    landing_path: str           # Origen de los ficheros crudos
    raw_path: str               # Archivo inmutable tras la ingesta
    bronze_path: str            # Tablas Delta de destino
    kafka_bootstrap_servers: Optional[str] = None
    kafka_security_protocol: Optional[str] = None
    kafka_sasl_mechanism: Optional[str] = None
    kafka_sasl_username: Optional[str] = None
    kafka_sasl_password: Optional[str] = None
    schema_registry_url: Optional[str] = None
    schema_registry_username: Optional[str] = None
    schema_registry_password: Optional[str] = None

    @classmethod
    def from_dict(cls, d: dict) -> "Environment":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})

    def kafka_spark_opts(self) -> dict:
        """Devuelve las opciones de Spark necesarias para conectarse a Kafka."""
        return {
            "kafka.bootstrap.servers": self.kafka_bootstrap_servers,
            "kafka.security.protocol": self.kafka_security_protocol,
            "kafka.sasl.mechanism": self.kafka_sasl_mechanism,
            "kafka.sasl.jaas.config": (
                f'kafkashaded.org.apache.kafka.common.security.plain.'
                f'PlainLoginModule required username="{self.kafka_sasl_username}" '
                f'password="{self.kafka_sasl_password}";'
            ),
        }

    def schema_registry_conf(self) -> dict:
        return {
            "url": self.schema_registry_url,
            "basic.auth.user.info": (
                f"{self.schema_registry_username}:{self.schema_registry_password}"
            ),
        }


# ---------------------------------------------------------------------------
# Configuración de fuente batch (ficheros)
# ---------------------------------------------------------------------------

@dataclass
class BatchSourceConfig:
    """
    Define cómo leer un dataset de ficheros desde la capa landing.

    Atributos
    ---------
    format : str
        Formato del fichero: csv, json, parquet, avro, binaryFile.
    use_autoloader : bool
        True  → usa Databricks Autoloader (cloudFiles). Requiere Databricks.
        False → usa el file source nativo de Spark (funciona en local).
    schema_hints : str, opcional
        Pistas de esquema para Autoloader (ej. "id long, name string").
    schema_evolution : bool
        Si True, permite añadir columnas nuevas sin romper la ingesta.
    options : dict
        Opciones adicionales que se pasan directamente al reader de Spark.
    partition_by : list[str]
        Columnas por las que particionar al escribir en bronze.
    """
    format: str
    use_autoloader: bool = True
    schema_hints: Optional[str] = None
    schema_evolution: bool = True
    options: dict = field(default_factory=dict)
    partition_by: list = field(default_factory=list)


# ---------------------------------------------------------------------------
# Configuración de fuente streaming (Kafka)
# ---------------------------------------------------------------------------

@dataclass
class StreamingSourceConfig:
    """
    Define cómo leer un dataset desde un topic de Kafka.

    Atributos
    ---------
    topic_pattern : str
        Nombre exacto del topic o patrón regex (ej. "orders", "farm\\..*").
    key_format : str
        Formato de la clave del mensaje: string, bytes.
    value_format : str
        Formato del valor del mensaje: json, avro, string.
    json_schema : str, opcional
        DDL del esquema cuando value_format es json
        (ej. "id long, name string, ts timestamp").
    key_subject : str, opcional
        Subject del Schema Registry para la clave (solo Avro).
    value_subject : str, opcional
        Subject del Schema Registry para el valor (solo Avro).
    starting_offsets : str
        Desde dónde empezar a leer: "earliest" o "latest".
    options : dict
        Opciones adicionales de Kafka para Spark.
    partition_by : list[str]
        Columnas por las que particionar al escribir en bronze.
    """
    topic_pattern: str
    key_format: str = "string"
    value_format: str = "json"
    json_schema: Optional[str] = None
    key_subject: Optional[str] = None
    value_subject: Optional[str] = None
    starting_offsets: str = "earliest"
    options: dict = field(default_factory=dict)
    partition_by: list = field(default_factory=list)


# ---------------------------------------------------------------------------
# Dataset — unidad de configuración que el motor procesa
# ---------------------------------------------------------------------------

@dataclass
class DatasetConfig:
    """
    Representa un dataset completo: de dónde viene y a dónde va.

    Atributos
    ---------
    datasource : str
        Nombre del sistema origen (ej. "farmia", "iot", "ecommerce").
    dataset : str
        Nombre del dataset dentro del datasource (ej. "sales", "sensors").
    source : BatchSourceConfig | StreamingSourceConfig
        Configuración de la fuente. El tipo determina si es batch o streaming.
    """
    datasource: str
    dataset: str
    source: BatchSourceConfig | StreamingSourceConfig

    @property
    def is_streaming(self) -> bool:
        return isinstance(self.source, StreamingSourceConfig)

    @property
    def source_path(self) -> str:
        """Solo para batch: subcarpeta dentro de landing."""
        if self.is_streaming:
            raise ValueError("Los datasets streaming no tienen source_path.")
        return f"{self.datasource}/{self.dataset}"
