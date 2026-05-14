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

    Dos modos según la configuración del Environment:

    - **Modo Unity Catalog** (cuando bronze_catalog y bronze_schema están
      definidos): usa `.toTable("catalog.schema.datasource__dataset")`,
      que crea una tabla **managed** en UC. La tabla queda registrada
      automáticamente y consultable desde el SQL Editor. Recomendado en
      Databricks (Free Edition incluido).

    - **Modo path** (fallback, sin catalog/schema): usa `.start(bronze_path)`
      escribiendo a un path Delta. Útil para tests locales sin metastore.

    En ambos modos: trigger(availableNow=True), mergeSchema activado,
    sin foreachBatch — compatible con Serverless y Autoloader.

    Parámetros
    ----------
    spark : SparkSession
    env   : Environment
        Contiene las rutas base y, opcionalmente, catalog/schema de UC.
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
        Devuelve la StreamingQuery para que el motor pueda esperarla.
        """
        checkpoint_path = self._checkpoint_path(dataset)
        query_name = f"{dataset.datasource}__{dataset.dataset}"
        target = self._resolve_target(dataset)

        logger.info(f"[BronzeWriter] Iniciando query '{query_name}' → {target.label}")

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

        if target.is_uc_table:
            return writer.toTable(target.value)
        return writer.start(target.value)

    # ------------------------------------------------------------------
    # Resolución del sink (tabla UC managed o path)
    # ------------------------------------------------------------------

    def _resolve_target(self, dataset: DatasetConfig) -> "_BronzeTarget":
        catalog = self.env.bronze_catalog
        schema = self.env.bronze_schema

        if catalog and schema:
            # Asegura que el schema existe — la primera vez es necesario.
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
            fqn = f"{catalog}.{schema}.{self._table_name(dataset)}"
            return _BronzeTarget(value=fqn, is_uc_table=True, label=f"tabla UC {fqn}")

        path = self._bronze_path(dataset)
        return _BronzeTarget(value=path, is_uc_table=False, label=f"path {path}")

    # ------------------------------------------------------------------
    # Rutas y nombres auxiliares
    # ------------------------------------------------------------------

    def _bronze_path(self, dataset: DatasetConfig) -> str:
        return f"{self.env.bronze_path}/{dataset.datasource}/{dataset.dataset}"

    def _checkpoint_path(self, dataset: DatasetConfig) -> str:
        # El checkpoint vive siempre en el Volume — independiente del sink elegido,
        # ya que es estado de Spark Streaming y no del catálogo.
        return f"{self.env.bronze_path}/{dataset.datasource}/{dataset.dataset}_checkpoint"

    @staticmethod
    def _table_name(dataset: DatasetConfig) -> str:
        return f"{dataset.datasource}__{dataset.dataset}"


# ---------------------------------------------------------------------------
# Helper interno
# ---------------------------------------------------------------------------

from dataclasses import dataclass


@dataclass(frozen=True)
class _BronzeTarget:
    """Destino resuelto para la escritura: o bien una tabla UC, o bien un path."""
    value: str
    is_uc_table: bool
    label: str   # solo para logs
