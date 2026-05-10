import os
from pathlib import Path

# Forzar rutas temporales
Path("C:/tmp/spark").mkdir(parents=True, exist_ok=True)
os.environ["SPARK_LOCAL_DIRS"] = "C:/tmp/spark"
os.environ["HADOOP_HOME"] = r"C:\spark\winutils"

import logging
logging.basicConfig(level=logging.DEBUG)

from pyspark.sql import SparkSession

print("Intentando crear SparkSession...")
try:
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("test")
        .config("spark.local.dir", "C:/tmp/spark")
        .getOrCreate()
    )
    print("✅ SparkSession creada correctamente")
    spark.range(5).show()
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
