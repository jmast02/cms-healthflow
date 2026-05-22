"""SparkSession factory with standard CMS HealthFlow configuration."""

import os
import urllib.request
from pathlib import Path

from pyspark.sql import SparkSession

from spark.config import CONFIG

# PostgreSQL JDBC JAR — needed for Spark → PostgreSQL JDBC writes.
# Downloaded by Dockerfile.spark; if missing at runtime (e.g. dev volume mount),
# we fetch it automatically so the container is always self-healing.
_JDBC_JAR = Path("/opt/postgresql-jdbc.jar")
_JDBC_URL = "https://jdbc.postgresql.org/download/postgresql-42.7.3.jar"

if not _JDBC_JAR.exists():
    try:
        _JDBC_JAR.parent.mkdir(parents=True, exist_ok=True)
        print(f"Downloading PostgreSQL JDBC driver → {_JDBC_JAR}")
        urllib.request.urlretrieve(_JDBC_URL, str(_JDBC_JAR))
    except Exception as e:
        print(f"Warning: could not download JDBC JAR: {e}")

# PYSPARK_SUBMIT_ARGS must be set before the JVM starts.
# builder.config("spark.driver.extraClassPath", ...) is too late.
if _JDBC_JAR.exists():
    os.environ.setdefault("PYSPARK_SUBMIT_ARGS", f"--jars {_JDBC_JAR} pyspark-shell")


def get_spark_session(app_name: str | None = None) -> SparkSession:
    """Return a configured SparkSession.

    Uses local[*] mode by default — no Spark cluster required.
    All jobs write plain Parquet (snappy compressed).
    JDBC writes to PostgreSQL use the bundled postgresql-jdbc.jar.
    """
    name = app_name or CONFIG.app_name

    builder = (
        SparkSession.builder
        .appName(name)
        .master(CONFIG.master)
        .config("spark.driver.memory", CONFIG.driver_memory)
        .config("spark.executor.memory", CONFIG.executor_memory)
        # Adaptive query execution — helps with skewed CMS data
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Write Parquet with snappy compression
        .config("spark.sql.parquet.compression.codec", "snappy")
        # Suppress noisy INFO logs
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.ui.enabled", "false")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def stop_spark(spark: SparkSession) -> None:
    spark.stop()
