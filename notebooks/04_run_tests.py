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

import subprocess

result = subprocess.run(
    [
        sys.executable, "-m", "pytest",
        "tests/test_batch_reader.py",
        "-v",
        "--timeout=120",
        "--tb=short",
        "--no-header",
    ],
    capture_output=True,
    text=True,
    cwd=REPO_ROOT,
)
print(result.stdout)
if result.stderr:
    print(result.stderr)