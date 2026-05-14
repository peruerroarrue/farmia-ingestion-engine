"""
test_config.py
--------------
Tests para la carga de configuración desde YAML y la construcción
de los objetos tipados Environment y DatasetConfig.
No requieren SparkSession.
"""

import pytest
import yaml
from pathlib import Path

from src.config import (
    Environment,
    BatchSourceConfig,
    StreamingSourceConfig,
    DatasetConfig,
)
from src.environment import (
    load_config,
    _build_source,
    _build_environment,
    _resolve_env_vars,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

VALID_YAML = """
environment:
  landing_path: "/tmp/farmia/landing"
  raw_path: "/tmp/farmia/raw"
  bronze_path: "/tmp/farmia/bronze"

datasets:
  - datasource: ecommerce
    dataset: sales_orders
    source:
      type: batch
      format: json
      use_autoloader: false
      schema_evolution: true
      partition_by:
        - ingestion_date

  - datasource: iot
    dataset: sensor_readings
    source:
      type: streaming
      topic_pattern: "sensor_readings"
      key_format: string
      value_format: json
      json_schema: "sensor_id string, temperature double"
      starting_offsets: earliest
      partition_by:
        - field_zone
"""


@pytest.fixture()
def valid_yaml_file(tmp_path):
    path = tmp_path / "test_datasets.yml"
    path.write_text(VALID_YAML)
    return str(path)


# ---------------------------------------------------------------------------
# Tests de Environment
# ---------------------------------------------------------------------------

class TestEnvironment:

    def test_build_from_dict(self):
        env = _build_environment({
            "landing_path": "/landing",
            "raw_path": "/raw",
            "bronze_path": "/bronze",
        })
        assert env.landing_path == "/landing"
        assert env.raw_path == "/raw"
        assert env.bronze_path == "/bronze"

    def test_optional_kafka_fields_default_none(self):
        env = Environment(
            landing_path="/landing",
            raw_path="/raw",
            bronze_path="/bronze",
        )
        assert env.kafka_bootstrap_servers is None
        assert env.schema_registry_url is None

    def test_bronze_catalog_schema_default_none(self):
        env = Environment(landing_path="/l", bronze_path="/b")
        assert env.bronze_catalog is None
        assert env.bronze_schema is None

    def test_bronze_catalog_schema_from_dict(self):
        env = _build_environment({
            "landing_path": "/l",
            "bronze_path": "/b",
            "bronze_catalog": "workspace",
            "bronze_schema": "bronze",
        })
        assert env.bronze_catalog == "workspace"
        assert env.bronze_schema == "bronze"

    def test_kafka_spark_opts_builds_correctly(self):
        env = Environment(
            landing_path="/l", raw_path="/r", bronze_path="/b",
            kafka_bootstrap_servers="broker:9092",
            kafka_security_protocol="SASL_SSL",
            kafka_sasl_mechanism="PLAIN",
            kafka_sasl_username="user",
            kafka_sasl_password="pass",
        )
        opts = env.kafka_spark_opts()
        assert opts["kafka.bootstrap.servers"] == "broker:9092"
        assert "user" in opts["kafka.sasl.jaas.config"]
        assert "pass" in opts["kafka.sasl.jaas.config"]

    def test_schema_registry_conf(self):
        env = Environment(
            landing_path="/l", raw_path="/r", bronze_path="/b",
            schema_registry_url="https://sr.confluent.cloud",
            schema_registry_username="sruser",
            schema_registry_password="srpass",
        )
        conf = env.schema_registry_conf()
        assert conf["url"] == "https://sr.confluent.cloud"
        assert "sruser:srpass" in conf["basic.auth.user.info"]


# ---------------------------------------------------------------------------
# Tests de BatchSourceConfig
# ---------------------------------------------------------------------------

class TestBatchSourceConfig:

    def test_build_batch_source(self):
        source = _build_source({
            "type": "batch",
            "format": "json",
            "use_autoloader": False,
            "schema_evolution": True,
            "partition_by": ["ingestion_date"],
        })
        assert isinstance(source, BatchSourceConfig)
        assert source.format == "json"
        assert source.use_autoloader is False
        assert source.schema_evolution is True
        assert "ingestion_date" in source.partition_by

    def test_batch_defaults(self):
        source = _build_source({"type": "batch", "format": "csv"})
        assert source.use_autoloader is True
        assert source.schema_evolution is True
        assert source.options == {}
        assert source.partition_by == []

    def test_batch_with_options(self):
        source = _build_source({
            "type": "batch",
            "format": "csv",
            "options": {"header": "true", "delimiter": ","},
        })
        assert source.options["header"] == "true"
        assert source.options["delimiter"] == ","


# ---------------------------------------------------------------------------
# Tests de StreamingSourceConfig
# ---------------------------------------------------------------------------

class TestStreamingSourceConfig:

    def test_build_streaming_source(self):
        source = _build_source({
            "type": "streaming",
            "topic_pattern": "orders",
            "key_format": "string",
            "value_format": "json",
            "json_schema": "id long, name string",
            "starting_offsets": "earliest",
            "partition_by": ["event_type"],
        })
        assert isinstance(source, StreamingSourceConfig)
        assert source.topic_pattern == "orders"
        assert source.value_format == "json"
        assert source.json_schema == "id long, name string"

    def test_streaming_defaults(self):
        source = _build_source({
            "type": "streaming",
            "topic_pattern": "my_topic",
        })
        assert source.key_format == "string"
        assert source.value_format == "json"
        assert source.starting_offsets == "earliest"

    def test_streaming_avro_subjects(self):
        source = _build_source({
            "type": "streaming",
            "topic_pattern": "events",
            "value_format": "avro",
            "value_subject": "events-value",
        })
        assert source.value_format == "avro"
        assert source.value_subject == "events-value"


# ---------------------------------------------------------------------------
# Tests de DatasetConfig
# ---------------------------------------------------------------------------

class TestDatasetConfig:

    def test_is_streaming_false_for_batch(self):
        ds = DatasetConfig(
            datasource="ecommerce",
            dataset="sales",
            source=BatchSourceConfig(format="json"),
        )
        assert ds.is_streaming is False

    def test_is_streaming_true_for_kafka(self):
        ds = DatasetConfig(
            datasource="iot",
            dataset="sensors",
            source=StreamingSourceConfig(topic_pattern="sensors"),
        )
        assert ds.is_streaming is True

    def test_source_path_for_batch(self):
        ds = DatasetConfig(
            datasource="ecommerce",
            dataset="sales",
            source=BatchSourceConfig(format="json"),
        )
        assert ds.source_path == "ecommerce/sales"

    def test_source_path_raises_for_streaming(self):
        ds = DatasetConfig(
            datasource="iot",
            dataset="sensors",
            source=StreamingSourceConfig(topic_pattern="sensors"),
        )
        with pytest.raises(ValueError):
            _ = ds.source_path


# ---------------------------------------------------------------------------
# Tests de load_config
# ---------------------------------------------------------------------------

class TestLoadConfig:

    def test_loads_environment(self, valid_yaml_file):
        env, _ = load_config(valid_yaml_file)
        assert env.landing_path == "/tmp/farmia/landing"
        assert env.bronze_path == "/tmp/farmia/bronze"

    def test_loads_correct_number_of_datasets(self, valid_yaml_file):
        _, datasets = load_config(valid_yaml_file)
        assert len(datasets) == 2

    def test_first_dataset_is_batch(self, valid_yaml_file):
        _, datasets = load_config(valid_yaml_file)
        assert datasets[0].datasource == "ecommerce"
        assert datasets[0].is_streaming is False

    def test_second_dataset_is_streaming(self, valid_yaml_file):
        _, datasets = load_config(valid_yaml_file)
        assert datasets[1].datasource == "iot"
        assert datasets[1].is_streaming is True

    def test_resolves_env_vars_in_yaml(self, tmp_path, monkeypatch):
        monkeypatch.setenv("KAFKA_SASL_USERNAME", "test_user")
        monkeypatch.setenv("KAFKA_SASL_PASSWORD", "test_pass")
        yaml_with_placeholders = """
environment:
  landing_path: "/tmp/l"
  bronze_path: "/tmp/b"
  kafka_sasl_username: "${KAFKA_SASL_USERNAME}"
  kafka_sasl_password: "${KAFKA_SASL_PASSWORD}"
datasets:
  - datasource: test
    dataset: data
    source:
      type: batch
      format: json
"""
        path = tmp_path / "with_placeholders.yml"
        path.write_text(yaml_with_placeholders)
        env, _ = load_config(str(path))
        assert env.kafka_sasl_username == "test_user"
        assert env.kafka_sasl_password == "test_pass"

    def test_missing_env_var_raises(self, tmp_path, monkeypatch):
        monkeypatch.delenv("FARMIA_DOES_NOT_EXIST", raising=False)
        bad_yaml = """
environment:
  landing_path: "/tmp/l"
  bronze_path: "/tmp/b"
  kafka_sasl_password: "${FARMIA_DOES_NOT_EXIST}"
datasets: []
"""
        path = tmp_path / "missing_env.yml"
        path.write_text(bad_yaml)
        with pytest.raises(EnvironmentError, match="FARMIA_DOES_NOT_EXIST"):
            load_config(str(path))

    def test_unknown_source_type_raises(self, tmp_path):
        bad_yaml = """
environment:
  landing_path: "/l"
  raw_path: "/r"
  bronze_path: "/b"
datasets:
  - datasource: test
    dataset: data
    source:
      type: unknown_type
      format: json
"""
        path = tmp_path / "bad.yml"
        path.write_text(bad_yaml)
        with pytest.raises(ValueError, match="unknown_type"):
            load_config(str(path))
