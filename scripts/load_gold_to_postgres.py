"""
Load Gold Parquet files into PostgreSQL gold schema.

Reads the ranked provider profiles, procedure costs, and geographic cost
Parquet files produced by the Spark jobs and writes them to PostgreSQL
so the FastAPI layer can serve live data.

Run: docker compose run --rm spark-load
"""

import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

PARQUET_DIR = Path(os.getenv("CMS_PARQUET_DIR", "data/parquet"))
YEAR = int(os.getenv("CMS_PROVIDER_DATASET_YEAR", 2022))

JDBC_URL = (
    f"jdbc:postgresql://"
    f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
    f"{os.getenv('POSTGRES_PORT', '5432')}/"
    f"{os.getenv('POSTGRES_DB', 'cms_healthflow')}"
)
JDBC_PROPS = {
    "user": os.getenv("POSTGRES_USER", "healthflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "healthflow_secret"),
    "driver": "org.postgresql.Driver",
}


def load_table(spark, parquet_path: Path, table: str, mode: str = "overwrite") -> int:
    if not parquet_path.exists():
        log.warning("Parquet path not found, skipping: %s", parquet_path)
        return 0

    log.info("Loading %s → gold.%s", parquet_path, table)
    df = spark.read.parquet(str(parquet_path))

    (
        df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", f"gold.{table}")
        .option("user", JDBC_PROPS["user"])
        .option("password", JDBC_PROPS["password"])
        .option("driver", JDBC_PROPS["driver"])
        .mode(mode)
        .save()
    )

    count = df.count()
    log.info("Loaded %d rows → gold.%s", count, table)
    return count


def main() -> None:
    from spark.utils.session import get_spark_session, stop_spark

    spark = get_spark_session("cms-load-gold")

    total = 0
    try:
        # Provider profiles (ranked)
        ranked_path = PARQUET_DIR / "gold" / "provider_profiles_ranked" / str(YEAR)
        fallback_path = PARQUET_DIR / "gold" / "provider_profiles" / str(YEAR)
        provider_path = ranked_path if ranked_path.exists() else fallback_path
        total += load_table(spark, provider_path, "provider_profiles")

        # Procedure costs
        total += load_table(
            spark,
            PARQUET_DIR / "gold" / "procedure_costs" / str(YEAR),
            "procedure_costs",
        )

        # Geographic costs
        total += load_table(
            spark,
            PARQUET_DIR / "gold" / "cost_by_geography" / str(YEAR),
            "cost_by_geography",
        )

        log.info("Gold load complete. Total rows written: %d", total)
    except Exception as exc:
        log.error("Load failed: %s", exc)
        sys.exit(1)
    finally:
        stop_spark(spark)


if __name__ == "__main__":
    main()
