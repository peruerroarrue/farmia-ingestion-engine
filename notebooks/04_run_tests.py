# Databricks notebook source
# MAGIC %md
# MAGIC # Tests de Integración — FarmIA Ingestion Engine

# COMMAND ----------

%pip install pytest pytest-timeout
dbutils.library.restartPython()

# COMMAND ----------

import sys
REPO_ROOT = "/Workspace/Repos/peruerro@ucm.es/farmia-ingestion-engine"
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# COMMAND ----------

import sys
sys.path.insert(0, "/tmp/farmia-tests")

import pytest
ret = pytest.main([
    "tests/test_batch_reader.py",
    "-v",
    "--timeout=120",
    "--tb=short",
    "--no-header",
], plugins=[])