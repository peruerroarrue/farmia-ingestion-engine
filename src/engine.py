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
    engine = IngestionEngine(env, datasets)            # tolerante a fallos
    # o bien
    engine = IngestionEngine(env, datasets, fail_fast=True)  # corta al primer error

    result = engine.run()      # devuelve IngestionResult
                               # lanza IngestionError si alguna ingesta falló
"""

import logging
from dataclasses import dataclass, field
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


# ---------------------------------------------------------------------------
# Resultado y excepción
# ---------------------------------------------------------------------------

class IngestionError(RuntimeError):
    """
    Lanzada por IngestionEngine.run() cuando alguna ingesta falló.
    Permite que el job de Databricks termine con código != 0
    y sea marcado como FAILED por el scheduler.
    """


@dataclass
class IngestionResult:
    """
    Resultado de una ejecución del motor.

    Atributos
    ---------
    succeeded : list[str]
        Nombres de los datasets que terminaron correctamente (formato
        'datasource/dataset').
    failed : list[tuple[str, str]]
        Pares (nombre, mensaje de error) de los datasets que fallaron.
    """
    succeeded: list[str] = field(default_factory=list)
    failed: list[tuple[str, str]] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.succeeded) + len(self.failed)

    @property
    def all_ok(self) -> bool:
        return len(self.failed) == 0


# ---------------------------------------------------------------------------
# Motor
# ---------------------------------------------------------------------------

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
    fail_fast : bool, por defecto False
        Si True, corta la ejecución en cuanto una ingesta falla
        (al lanzar la query o durante la ejecución). Si False, intenta
        ejecutar todas las ingestas y al final lanza IngestionError si
        alguna falló.
    await_timeout_sec : int, por defecto 300
        Timeout en segundos de awaitTermination por query.
    """

    def __init__(
        self,
        env: Environment,
        datasets: list[DatasetConfig],
        spark: SparkSession | None = None,
        fail_fast: bool = False,
        await_timeout_sec: int = 300,
    ):
        self.env = env
        self.datasets = datasets
        self.spark = spark or self._build_spark_session()
        self.fail_fast = fail_fast
        self.await_timeout_sec = await_timeout_sec
        self._batch_reader = BatchReader(self.spark, env)
        self._streaming_reader = StreamingReader(self.spark, env)
        self._writer = BronzeWriter(self.spark, env)

    # ------------------------------------------------------------------
    # Punto de entrada principal
    # ------------------------------------------------------------------

    def run(self) -> IngestionResult:
        """
        Ejecuta la ingesta completa:
        1. Crea un streaming DataFrame por cada dataset.
        2. Lanza todas las queries en secuencia.
        3. Espera a que terminen e informa del resultado.

        Devuelve el IngestionResult agregado.
        Lanza IngestionError si alguna ingesta falló.
        """
        logger.info(
            f"🚀 Iniciando motor de ingesta — {len(self.datasets)} datasets "
            f"(fail_fast={self.fail_fast})"
        )

        result = IngestionResult()
        queries = self._start_queries(result)

        if queries:
            self._await_queries(queries, result)
        elif not result.failed:
            logger.warning("No se lanzó ninguna query. Revisa la configuración.")

        self._print_summary(result)

        if not result.all_ok:
            raise IngestionError(
                f"{len(result.failed)}/{result.total} ingestas fallaron: "
                f"{[name for name, _ in result.failed]}"
            )

        return result

    # ------------------------------------------------------------------
    # Creación y lanzamiento de queries
    # ------------------------------------------------------------------

    def _start_queries(
        self,
        result: IngestionResult,
    ) -> list[tuple[DatasetConfig, StreamingQuery]]:
        """
        Itera sobre los datasets, crea el DataFrame correspondiente
        y lanza la streaming query. Devuelve los pares (config, query).
        Los datasets que fallan al lanzar quedan registrados en result.failed.
        """
        queries = []
        for dataset in self.datasets:
            name = f"{dataset.datasource}/{dataset.dataset}"
            try:
                df = self._read(dataset)
                query = self._writer.write(dataset, df)
                queries.append((dataset, query))
                logger.info(f"▶️  Query lanzada: {name}")
            except Exception as e:
                logger.error(f"❌ Error al lanzar {name}: {e}", exc_info=True)
                result.failed.append((name, str(e)))
                if self.fail_fast:
                    raise IngestionError(
                        f"fail_fast: '{name}' falló al lanzar la query"
                    ) from e
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
        result: IngestionResult,
    ) -> None:
        for dataset, query in queries:
            name = f"{dataset.datasource}/{dataset.dataset}"
            try:
                query.awaitTermination(timeout=self.await_timeout_sec)
                if query.exception() is None:
                    self._log_progress(name, query)
                    self._register_table(dataset, name)
                    result.succeeded.append(name)
                    logger.info(f"✅ Completada: {name}")
                else:
                    err = str(query.exception())
                    result.failed.append((name, err))
                    logger.error(f"❌ Fallida: {name} — {err}")
                    if self.fail_fast:
                        raise IngestionError(
                            f"fail_fast: '{name}' falló durante la ejecución"
                        )
            except IngestionError:
                raise
            except Exception as e:
                result.failed.append((name, str(e)))
                logger.error(f"❌ Fallida: {name} — {e}", exc_info=True)
                if self.fail_fast:
                    raise IngestionError(
                        f"fail_fast: '{name}' falló durante la espera"
                    ) from e

    def _register_table(self, dataset: DatasetConfig, name: str) -> None:
        """
        Registra el dataset como tabla externa en Unity Catalog.
        El fallo no es fatal — la ingesta a Bronze ya tuvo éxito; solo
        se pierde la posibilidad de consultarlo por nombre desde SQL.
        """
        try:
            fqn = self._writer.register_table(dataset)
            if fqn:
                logger.info(f"   📋 Tabla registrada en UC: {fqn}")
        except Exception as e:
            logger.warning(
                f"   ⚠️  No se pudo registrar la tabla UC de {name}: {e}"
            )

    def _log_progress(self, name: str, query: StreamingQuery) -> None:
        """
        Loguea métricas agregadas de la query (rows, batches, tiempo).
        Suma por todos los microbatches recientes para obtener el total.
        """
        recent = query.recentProgress
        if not recent:
            logger.info(f"   📊 {name} | sin métricas disponibles")
            return

        total_rows = sum(p.get("numInputRows", 0) for p in recent)
        n_batches = len(recent)
        duration_ms = sum(
            p.get("durationMs", {}).get("triggerExecution", 0) for p in recent
        )
        logger.info(
            f"   📊 {name} | batches={n_batches} | rows={total_rows} "
            f"| duration_ms={duration_ms}"
        )

    # ------------------------------------------------------------------
    # Resumen final
    # ------------------------------------------------------------------

    def _print_summary(self, result: IngestionResult) -> None:
        logger.info("─" * 60)
        logger.info(
            f"📊 Resumen: {len(result.succeeded)}/{result.total} ingestas completadas"
        )
        for name in result.succeeded:
            logger.info(f"   ✅ {name}")
        for name, err in result.failed:
            # Truncamos el error para que el resumen quepa en pantalla
            err_short = err.split("\n", 1)[0][:120]
            logger.error(f"   ❌ {name} — {err_short}")
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
