"""
writer.py
---------
Clase responsable de escribir streaming DataFrames en la capa bronze
en formato Delta, y de archivar los ficheros procesados en la capa raw.

Clase
-----
- BronzeWriter: escribe un DataFrame en bronze y archiva los ficheros origen.
"""

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import StreamingQuery

from src.config import Environment, DatasetConfig, BatchSourceConfig

logger = logging.getLogger(__name__)


class BronzeWriter:
    """
    Escribe un streaming DataFrame en la capa bronze en formato Delta.

    Para datasets batch también archiva los ficheros procesados
    moviéndolos de landing a raw, manteniendo la misma estructura
    de subcarpetas.

    Parámetros
    ----------
    spark : SparkSession
    env   : Environment
        Contiene las rutas base de landing, raw y bronze.
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

        Para batch usa foreachBatch para poder archivar ficheros tras
        cada microbatch. Para streaming escribe directamente.
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

        if dataset.is_streaming or source.use_autoloader:
            query = writer.start(bronze_path)
        else:
            # foreachBatch solo para batch sin autoloader
            def append_batch(batch_df: DataFrame, batch_id: int) -> None:
                self._write_batch(batch_df, bronze_path, source)
                if source.format != "binaryFile":
                    self._archive_files(batch_df, dataset, raw_path)

            query = (
                df.writeStream
                .option("checkpointLocation", checkpoint_path)
                .queryName(query_name)
                .trigger(availableNow=True)
                .foreachBatch(append_batch)
                .start()
            )

        return query

    # ------------------------------------------------------------------
    # Escritura del microbatch en bronze
    # ------------------------------------------------------------------

    def _write_batch(
            self,
            batch_df: DataFrame,
            bronze_path: str,
            source: BatchSourceConfig,
    ) -> None:
        """Escribe un microbatch en bronze en formato Delta."""
        (
            batch_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", str(source.schema_evolution).lower())
            .save(bronze_path)
        )
        logger.info(f"[BronzeWriter] Microbatch escrito en {bronze_path}")

    # ------------------------------------------------------------------
    # Archivado de ficheros: landing → raw
    # ------------------------------------------------------------------

    def _archive_files(
        self,
        batch_df: DataFrame,
        dataset: DatasetConfig,
        raw_path: str,
    ) -> None:
        """
        Mueve los ficheros procesados de landing a raw manteniendo
        la misma estructura de subcarpetas.

        Solo se ejecuta si el DataFrame tiene la columna _ingested_filename,
        que BatchReader añade para formatos no-binaryFile.
        """
        if "_ingested_filename" not in batch_df.columns:
            return

        landing_base = f"{self.env.landing_path}/{dataset.datasource}/{dataset.dataset}"
        raw_base = raw_path

        filenames = [
            row["_ingested_filename"]
            for row in batch_df.select("_ingested_filename").distinct().collect()
            if row["_ingested_filename"]
        ]

        if not filenames:
            return

        try:
            # dbutils solo está disponible en Databricks
            dbutils = self._get_dbutils()
            for src_path in filenames:
                # Reconstruimos la ruta raw manteniendo la subcarpeta
                relative = src_path.replace(landing_base, "").lstrip("/")
                dst_path = f"{raw_base}/{relative}"
                dst_dir = dst_path.rsplit("/", 1)[0]
                dbutils.fs.mkdirs(dst_dir)
                dbutils.fs.mv(src_path, dst_path)
                logger.info(f"[BronzeWriter] Archivado: {src_path} → {dst_path}")
        except Exception as e:
            # En local no hay dbutils; logueamos y continuamos
            logger.warning(
                f"[BronzeWriter] No se pudieron archivar ficheros "
                f"(dbutils no disponible en local): {e}"
            )

    def _get_dbutils(self):
        """Obtiene dbutils desde el contexto de Databricks."""
        from pyspark import SparkContext
        sc: SparkContext = self.spark.sparkContext
        return sc._jvm.com.databricks.dbutils_v1.DBUtilsHolder.dbutils0().get()

    # ------------------------------------------------------------------
    # Rutas auxiliares
    # ------------------------------------------------------------------

    def _bronze_path(self, dataset: DatasetConfig) -> str:
        return f"{self.env.bronze_path}/{dataset.datasource}/{dataset.dataset}"

    def _raw_path(self, dataset: DatasetConfig) -> str:
        return f"{self.env.raw_path}/{dataset.datasource}/{dataset.dataset}"

    def _checkpoint_path(self, dataset: DatasetConfig) -> str:
        return f"{self.env.bronze_path}/{dataset.datasource}/{dataset.dataset}_checkpoint"
