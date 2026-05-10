"""
environment.py
--------------
Carga la configuración del entorno y de los datasets desde un fichero YAML
y construye los objetos tipados que el motor de ingesta necesita.

Uso típico
----------
    env, datasets = load_config("configs/datasets.yml")
    engine = IngestionEngine(env, datasets)
    engine.run()
"""

import yaml
from pathlib import Path
from src.config import (
    Environment,
    BatchSourceConfig,
    StreamingSourceConfig,
    DatasetConfig,
)


# ---------------------------------------------------------------------------
# Carga del YAML
# ---------------------------------------------------------------------------

def _load_yaml(path: str | Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Construcción de la fuente (batch o streaming)
# ---------------------------------------------------------------------------

def _build_source(source_dict: dict) -> BatchSourceConfig | StreamingSourceConfig:
    source_type = source_dict.get("type")

    if source_type == "batch":
        return BatchSourceConfig(
            format=source_dict["format"],
            use_autoloader=source_dict.get("use_autoloader", True),
            schema_hints=source_dict.get("schema_hints"),
            schema_evolution=source_dict.get("schema_evolution", True),
            options=source_dict.get("options", {}),
            partition_by=source_dict.get("partition_by", []),
        )

    elif source_type == "streaming":
        return StreamingSourceConfig(
            topic_pattern=source_dict["topic_pattern"],
            key_format=source_dict.get("key_format", "string"),
            value_format=source_dict.get("value_format", "json"),
            json_schema=source_dict.get("json_schema"),
            key_subject=source_dict.get("key_subject"),
            value_subject=source_dict.get("value_subject"),
            starting_offsets=source_dict.get("starting_offsets", "earliest"),
            options=source_dict.get("options", {}),
            partition_by=source_dict.get("partition_by", []),
        )

    else:
        raise ValueError(
            f"Tipo de fuente desconocido: '{source_type}'. "
            f"Usa 'batch' o 'streaming'."
        )


# ---------------------------------------------------------------------------
# Construcción de los datasets
# ---------------------------------------------------------------------------

def _build_datasets(datasets_list: list[dict]) -> list[DatasetConfig]:
    datasets = []
    for d in datasets_list:
        dataset = DatasetConfig(
            datasource=d["datasource"],
            dataset=d["dataset"],
            source=_build_source(d["source"]),
        )
        datasets.append(dataset)
    return datasets


# ---------------------------------------------------------------------------
# Construcción del entorno
# ---------------------------------------------------------------------------

def _build_environment(env_dict: dict) -> Environment:
    return Environment.from_dict(env_dict)


# ---------------------------------------------------------------------------
# Punto de entrada principal
# ---------------------------------------------------------------------------

def load_config(path: str | Path) -> tuple[Environment, list[DatasetConfig]]:
    """
    Lee el YAML de configuración y devuelve el entorno y la lista de datasets.

    Parámetros
    ----------
    path : str | Path
        Ruta al fichero YAML (ej. "configs/datasets.yml").

    Retorna
    -------
    env : Environment
        Objeto con las rutas base y credenciales del entorno.
    datasets : list[DatasetConfig]
        Lista de datasets a ingestar.
    """
    raw = _load_yaml(path)

    env = _build_environment(raw["environment"])
    datasets = _build_datasets(raw["datasets"])

    print(f"✅ Configuración cargada: {len(datasets)} datasets")
    for ds in datasets:
        mode = "streaming" if ds.is_streaming else "batch"
        print(f"   · {ds.datasource}/{ds.dataset} [{mode}]")

    return env, datasets
