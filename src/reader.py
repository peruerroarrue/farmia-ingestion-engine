"""
reader.py
---------
Clases responsables de crear streaming DataFrames a partir de la
configuración de cada dataset.

Clases
------
- BatchReader    : lee ficheros desde landing usando Spark o Autoloader.
- StreamingReader: lee mensajes desde Kafka (JSON o Avro).
"""

import logging
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

from src.config import Environment, DatasetConfig, BatchSourceConfig, StreamingSourceConfig

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# BatchReader
# ---------------------------------------------------------------------------

class BatchReader:
    """
    Crea un streaming DataFrame desde la capa landing para datasets batch.

    Soporta los formatos: json, csv, parquet, avro, binaryFile.
    Puede usar Databricks Autoloader (cloudFiles) o el file source nativo.
    Añade metadatos de ingesta a cada registro.
    """

    def __init__(self, spark: SparkSession, env: Environment):
        self.spark = spark
        self.env = env

    def read(self, dataset: DatasetConfig) -> DataFrame:
        source: BatchSourceConfig = dataset.source
        landing_path = f"{self.env.landing_path}/{dataset.datasource}/{dataset.dataset}"
        schema_location = f"{self.env.bronze_path}/{dataset.datasource}/{dataset.dataset}_schema"

        logger.info(f"[BatchReader] {dataset.datasource}/{dataset.dataset} "
                    f"| format={source.format} | autoloader={source.use_autoloader}")

        if source.use_autoloader:
            df = self._read_autoloader(source, landing_path, schema_location)
        else:
            df = self._read_spark(source, landing_path)

        return self._add_metadata(df, dataset)

    # ------------------------------------------------------------------
    # Autoloader (solo Databricks)
    # ------------------------------------------------------------------

    def _read_autoloader(
        self,
        source: BatchSourceConfig,
        landing_path: str,
        schema_location: str,
    ) -> DataFrame:
        opts = {
            "cloudFiles.format": source.format,
            "cloudFiles.schemaLocation": schema_location,
        }

        if source.schema_hints:
            opts["cloudFiles.schemaHints"] = source.schema_hints

        if source.schema_evolution:
            opts["cloudFiles.schemaEvolutionMode"] = "addNewColumns"
        else:
            opts["cloudFiles.schemaEvolutionMode"] = "none"

        opts.update(source.options)

        reader = self.spark.readStream.format("cloudFiles").options(**opts)

        if source.format == "binaryFile":
            reader = reader.option("recursiveFileLookup", "true")

        return reader.load(landing_path)

    # ------------------------------------------------------------------
    # Spark file source nativo (local + Databricks)
    # ------------------------------------------------------------------

    def _read_spark(self, source: BatchSourceConfig, landing_path: str) -> DataFrame:
        fmt = source.format

        if fmt == "binaryFile":
            from pyspark.sql.types import (
                StructType, StructField, StringType,
                LongType, TimestampType, BinaryType
            )
            binary_schema = StructType([
                StructField("path", StringType()),
                StructField("modificationTime", TimestampType()),
                StructField("length", LongType()),
                StructField("content", BinaryType()),
            ])
            return (
                self.spark.readStream
                .format("binaryFile")
                .schema(binary_schema)
                .options(**source.options)
                .load(landing_path)
            )

        # Para el resto de formatos inferimos schema en la primera lectura
        # usando una lectura estática de muestra, y luego lanzamos el stream.
        inferred_schema = (
            self.spark.read
            .format(fmt)
            .options(**source.options)
            .load(landing_path)
            .schema
        )

        opts = {"maxFilesPerTrigger": "1"}
        opts.update(source.options)

        return (
            self.spark.readStream
            .format(fmt)
            .schema(inferred_schema)
            .options(**opts)
            .load(landing_path)
        )

    # ------------------------------------------------------------------
    # Metadatos de ingesta
    # ------------------------------------------------------------------

    def _add_metadata(self, df: DataFrame, dataset: DatasetConfig) -> DataFrame:
        source: BatchSourceConfig = dataset.source

        df = df.withColumn("_ingested_at", F.current_timestamp())
        df = df.withColumn("_datasource", F.lit(dataset.datasource))
        df = df.withColumn("_dataset", F.lit(dataset.dataset))

        # Nombre del fichero origen (no disponible en binaryFile, ya tiene 'path')
        if source.format != "binaryFile":
            df = df.withColumn("_ingested_filename", F.col("_metadata.file_path"))

        # Columna de fecha para particionado
        df = df.withColumn("ingestion_date", F.to_date(F.col("_ingested_at")))

        return df


# ---------------------------------------------------------------------------
# StreamingReader
# ---------------------------------------------------------------------------

class StreamingReader:
    """
    Crea un streaming DataFrame desde un topic de Kafka.

    Soporta valores en formato JSON y Avro (con Schema Registry).
    Añade metadatos de ingesta a cada registro.
    """

    def __init__(self, spark: SparkSession, env: Environment):
        self.spark = spark
        self.env = env

    def read(self, dataset: DatasetConfig) -> DataFrame:
        source: StreamingSourceConfig = dataset.source

        logger.info(f"[StreamingReader] {dataset.datasource}/{dataset.dataset} "
                    f"| topic={source.topic_pattern} | value_format={source.value_format}")

        df = self._read_kafka(source)
        df = self._decode_key(df, source)
        df = self._decode_value(df, source)
        return self._add_metadata(df, dataset)

    # ------------------------------------------------------------------
    # Lectura base desde Kafka
    # ------------------------------------------------------------------

    def _read_kafka(self, source: StreamingSourceConfig) -> DataFrame:
        opts = self.env.kafka_spark_opts()
        opts["startingOffsets"] = source.starting_offsets

        # subscribePattern permite regex; subscribe es topic exacto
        if any(c in source.topic_pattern for c in r".*+?[](){}\\^$|"):
            opts["subscribePattern"] = source.topic_pattern
        else:
            opts["subscribe"] = source.topic_pattern

        opts.update(source.options)

        return (
            self.spark.readStream
            .format("kafka")
            .options(**opts)
            .load()
        )

    # ------------------------------------------------------------------
    # Decodificación de clave y valor
    # ------------------------------------------------------------------

    def _decode_key(self, df: DataFrame, source: StreamingSourceConfig) -> DataFrame:
        if source.key_format == "string":
            return df.withColumn("key", F.col("key").cast("string"))
        return df  # bytes: se deja como está

    def _decode_value(self, df: DataFrame, source: StreamingSourceConfig) -> DataFrame:
        fmt = source.value_format

        if fmt == "json":
            schema = source.json_schema
            df = df.withColumn(
                "value",
                F.from_json(F.col("value").cast("string"), schema)
            )
            # Extraemos los campos del value al nivel raíz
            return df.select("*", "value.*").drop("value")

        elif fmt == "avro":
            return self._decode_avro(df, source)

        elif fmt == "string":
            return df.withColumn("value", F.col("value").cast("string"))

        else:
            raise ValueError(f"value_format no soportado: '{fmt}'")

    def _decode_avro(self, df: DataFrame, source: StreamingSourceConfig) -> DataFrame:
        try:
            from pyspark.sql.avro.functions import from_avro
            from confluent_kafka.schema_registry import SchemaRegistryClient
        except ImportError as e:
            raise ImportError(
                "Para Avro necesitas: pip install confluent-kafka[avro]"
            ) from e

        sr_client = SchemaRegistryClient(self.env.schema_registry_conf())
        subject = source.value_subject or f"{source.topic_pattern}-value"
        schema_str = sr_client.get_latest_version(subject).schema.schema_str

        # Los primeros 5 bytes son la magic byte + schema ID de Confluent
        return df.withColumn(
            "value",
            from_avro(F.expr("substring(value, 6, length(value) - 5)"), schema_str)
        )

    # ------------------------------------------------------------------
    # Metadatos de ingesta
    # ------------------------------------------------------------------

    def _add_metadata(self, df: DataFrame, dataset: DatasetConfig) -> DataFrame:
        return (
            df
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_datasource", F.lit(dataset.datasource))
            .withColumn("_dataset", F.lit(dataset.dataset))
            .withColumn("_kafka_topic", F.col("topic"))
            .withColumn("_kafka_offset", F.col("offset"))
            .withColumn("_kafka_partition", F.col("partition"))
        )
