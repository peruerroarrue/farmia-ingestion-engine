"""
engine.py
---------
Orquestador principal del motor de ingesta.

Recibe el entorno y la lista de datasets, crea los readers y writers
apropiados para cada uno, lanza todas las streaming queries en paralelo
y espera a que terminen.

Uso típico
----------
    from src.environment import load_config
    from src.engine import IngestionEngine

    env, datasets = load_config("configs/datasets.yml")
    engine = IngestionEngine(env, datasets)
    engine.run()
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

from src.config import Environment, DatasetConfig
from src.reader import BatchReader, StreamingReader
from src.writer import BronzeWriter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


class IngestionEngine:
    """
    Motor de ingesta de FarmIA.

    Orquesta la lectura desde landing/Kafka y la escritura en bronze
    para todos los datasets configurados.

    Parámetros
    ----------
    env : Environment
        Rutas base y credenciales del entorno.
    datasets : list[DatasetConfig]
        Lista de datasets a ingestar.
    spark : SparkSession, opcional
        Si no se proporciona, el motor crea una sesión local con Delta.
    """

    def __init__(
        self,
        env: Environment,
        datasets: list[DatasetConfig],
        spark: SparkSession | None = None,
    ):
        self.env = env
        self.datasets = datasets
        self.spark = spark or self._build_spark_session()
        self._batch_reader = BatchReader(self.spark, env)
        self._streaming_reader = StreamingReader(self.spark, env)
        self._writer = BronzeWriter(self.spark, env)

    # ------------------------------------------------------------------
    # Punto de entrada principal
    # ------------------------------------------------------------------

    def run(self) -> None:
        """
        Ejecuta la ingesta completa:
        1. Crea un streaming DataFrame por cada dataset.
        2. Lanza todas las queries en paralelo.
        3. Espera a que terminen e informa del resultado.
        """
        logger.info(f"🚀 Iniciando motor de ingesta — {len(self.datasets)} datasets")

        queries = self._start_queries()

        if not queries:
            logger.warning("No se lanzó ninguna query. Revisa la configuración.")
            return

        self._await_queries(queries)

    # ------------------------------------------------------------------
    # Creación y lanzamiento de queries
    # ------------------------------------------------------------------

    def _start_queries(self) -> list[tuple[DatasetConfig, StreamingQuery]]:
        """
        Itera sobre los datasets, crea el DataFrame correspondiente
        y lanza la streaming query. Devuelve los pares (config, query).
        """
        queries = []
        for dataset in self.datasets:
            try:
                df = self._read(dataset)
                query = self._writer.write(dataset, df)
                queries.append((dataset, query))
                logger.info(
                    f"▶️  Query lanzada: {dataset.datasource}/{dataset.dataset}"
                )
            except Exception as e:
                logger.error(
                    f"❌ Error al lanzar {dataset.datasource}/{dataset.dataset}: {e}",
                    exc_info=True,
                )
        return queries

    def _read(self, dataset: DatasetConfig):
        """Selecciona el reader adecuado según el tipo de dataset."""
        if dataset.is_streaming:
            return self._streaming_reader.read(dataset)
        return self._batch_reader.read(dataset)

    # ------------------------------------------------------------------
    # Espera de queries
    # ------------------------------------------------------------------

    def _await_queries(
        self,
        queries: list[tuple[DatasetConfig, StreamingQuery]],
    ) -> None:
        """
        Espera a que todas las queries terminen e imprime un resumen.
        Captura errores individuales sin detener el resto de queries.
        """
        succeeded = []
        failed = []

        for dataset, query in queries:
            name = f"{dataset.datasource}/{dataset.dataset}"
            try:
                query.awaitTermination()
                succeeded.append(name)
                logger.info(f"✅ Completada: {name}")
            except Exception as e:
                failed.append(name)
                logger.error(f"❌ Fallida: {name} — {e}", exc_info=True)

        self._print_summary(succeeded, failed)

    # ------------------------------------------------------------------
    # Resumen final
    # ------------------------------------------------------------------

    def _print_summary(self, succeeded: list[str], failed: list[str]) -> None:
        total = len(succeeded) + len(failed)
        logger.info("─" * 60)
        logger.info(f"📊 Resumen: {len(succeeded)}/{total} ingestas completadas")
        for name in succeeded:
            logger.info(f"   ✅ {name}")
        for name in failed:
            logger.error(f"   ❌ {name}")
        logger.info("─" * 60)

    # ------------------------------------------------------------------
    # SparkSession local con soporte Delta
    # ------------------------------------------------------------------

    @staticmethod
    def _build_spark_session() -> SparkSession:
        """
        Crea una SparkSession local con soporte para Delta Lake.
        Solo se usa si no se inyecta una sesión externa (ej. en Databricks
        ya existe una sesión activa llamada 'spark').
        """
        try:
            from delta import configure_spark_with_delta_pip
            builder = (
                SparkSession.builder
                .master("local[*]")
                .appName("FarmIA Ingestion Engine")
                .config("spark.sql.extensions",
                        "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog",
                        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            )
            spark = configure_spark_with_delta_pip(builder).getOrCreate()
        except ImportError:
            # Sin delta-spark instalado arranca sin Delta (útil para tests básicos)
            spark = (
                SparkSession.builder
                .master("local[*]")
                .appName("FarmIA Ingestion Engine")
                .getOrCreate()
            )
        spark.sparkContext.setLogLevel("WARN")
        return spark
