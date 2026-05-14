"""
writer.py
---------
Clase responsable de escribir streaming DataFrames en la capa bronze
en formato Delta.

Clase
-----
- BronzeWriter: escribe un streaming DataFrame en bronze.
"""

import logging
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import StreamingQuery

from src.config import Environment, DatasetConfig

logger = logging.getLogger(__name__)


class BronzeWriter:
    """
    Escribe un streaming DataFrame en la capa bronze en formato Delta.

    Mismo patrón para batch y streaming: writeStream.start(bronze_path)
    con trigger(availableNow=True). Compatible con Databricks Serverless,
    Autoloader y cluster clásico sin necesidad de foreachBatch.

    Parámetros
    ----------
    spark : SparkSession
    env   : Environment
        Contiene las rutas base de landing y bronze.
    """

    def __init__(self, spark: SparkSession, env: Environment):
        self.spark = spark
        self.env = env

    # ------------------------------------------------------------------
    # Punto de entrada principal
    # ------------------------------------------------------------------

    def write(self, dataset: DatasetConfig, df: DataFrame) -> StreamingQuery:
        """
        Lanza la streaming query que escribe el DataFrame en bronze.
        """
        bronze_path = self._bronze_path(dataset)
        checkpoint_path = self._checkpoint_path(dataset)
        query_name = f"{dataset.datasource}__{dataset.dataset}"

        logger.info(f"[BronzeWriter] Iniciando query '{query_name}' → {bronze_path}")

        writer = (
            df.writeStream
            .format("delta")
            .option("checkpointLocation", checkpoint_path)
            .option("mergeSchema", "true")
            .queryName(query_name)
            .trigger(availableNow=True)
        )

        if dataset.source.partition_by:
            writer = writer.partitionBy(*dataset.source.partition_by)

        return writer.start(bronze_path)

    # ------------------------------------------------------------------
    # Registro de tabla externa en Unity Catalog
    # ------------------------------------------------------------------

    def register_table(self, dataset: DatasetConfig) -> Optional[str]:
        """
        Registra el path de Bronze como tabla externa en Unity Catalog,
        permitiendo consultar el dataset desde el SQL Editor con:

            SELECT * FROM <catalog>.<schema>.<datasource>__<dataset>

        Devuelve el FQN de la tabla, o None si no hay catalog/schema
        configurado en el Environment (caso de tests locales sin UC).

        Es idempotente: usa CREATE TABLE IF NOT EXISTS, así que se puede
        ejecutar tras cada ingesta sin efecto secundario si la tabla ya existe.
        """
        catalog = self.env.bronze_catalog
        schema = self.env.bronze_schema
        if not catalog or not schema:
            return None

        table = self._table_name(dataset)
        fqn = f"{catalog}.{schema}.{table}"
        path = self._bronze_path(dataset)

        # Asegura que el schema existe — necesario la primera vez en Free Edition
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
        # Tabla externa: el data sigue en el path del Volume, UC solo guarda el puntero
        self.spark.sql(
            f"CREATE TABLE IF NOT EXISTS {fqn} USING DELTA LOCATION '{path}'"
        )
        return fqn

    # ------------------------------------------------------------------
    # Rutas y nombres auxiliares
    # ------------------------------------------------------------------

    def _bronze_path(self, dataset: DatasetConfig) -> str:
        return f"{self.env.bronze_path}/{dataset.datasource}/{dataset.dataset}"

    def _checkpoint_path(self, dataset: DatasetConfig) -> str:
        return f"{self.env.bronze_path}/{dataset.datasource}/{dataset.dataset}_checkpoint"

    @staticmethod
    def _table_name(dataset: DatasetConfig) -> str:
        return f"{dataset.datasource}__{dataset.dataset}"
