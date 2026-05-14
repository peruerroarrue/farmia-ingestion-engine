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
    # Rutas auxiliares
    # ------------------------------------------------------------------

    def _bronze_path(self, dataset: DatasetConfig) -> str:
        return f"{self.env.bronze_path}/{dataset.datasource}/{dataset.dataset}"

    def _checkpoint_path(self, dataset: DatasetConfig) -> str:
        return f"{self.env.bronze_path}/{dataset.datasource}/{dataset.dataset}_checkpoint"
