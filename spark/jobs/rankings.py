"""
Spark Job — Provider Rankings (Gold enrichment)

Adds specialty_rank and state_rank to the gold provider_profiles table
using PySpark window functions.

Run:  python -m spark.jobs.rankings
"""

import logging
import sys

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from spark.config import CONFIG
from spark.utils.session import get_spark_session, stop_spark

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def read_gold_providers(spark, year: int) -> DataFrame:
    path = f"{CONFIG.parquet_dir}/gold/provider_profiles/{year}"
    log.info("Reading Gold provider_profiles: %s", path)
    return spark.read.parquet(path)


def add_specialty_rank(df: DataFrame) -> DataFrame:
    """Rank providers by avg_medicare_payment within each provider_type."""
    window = Window.partitionBy("provider_type").orderBy(F.col("avg_medicare_payment").desc())
    return df.withColumn("specialty_rank", F.rank().over(window))


def add_state_rank(df: DataFrame) -> DataFrame:
    """Rank providers by avg_medicare_payment within each state."""
    window = Window.partitionBy("provider_state").orderBy(F.col("avg_medicare_payment").desc())
    return df.withColumn("state_rank", F.rank().over(window))


def add_percentile_in_specialty(df: DataFrame) -> DataFrame:
    """Compute what percentile of avg payment each provider sits in within their specialty."""
    window = Window.partitionBy("provider_type")
    total = F.count("provider_npi").over(window)
    rank = F.rank().over(Window.partitionBy("provider_type").orderBy(F.col("avg_medicare_payment")))
    return df.withColumn(
        "specialty_payment_percentile",
        F.round((rank / total) * 100, 1),
    )


def main() -> None:
    spark = get_spark_session("cms-rankings")
    try:
        year = CONFIG.dataset_year
        df = read_gold_providers(spark, year)

        log.info("Computing specialty rankings...")
        df = add_specialty_rank(df)

        log.info("Computing state rankings...")
        df = add_state_rank(df)

        log.info("Computing specialty payment percentiles...")
        df = add_percentile_in_specialty(df)

        out_path = f"{CONFIG.parquet_dir}/gold/provider_profiles_ranked/{year}"
        log.info("Writing ranked profiles → %s", out_path)
        df.write.mode("overwrite").parquet(out_path)

        log.info("Rankings job complete. Sample:")
        df.select(
            "provider_npi", "provider_name", "provider_type",
            "avg_medicare_payment", "specialty_rank", "state_rank",
        ).orderBy("provider_type", "specialty_rank").show(10, truncate=False)
    except Exception as exc:
        log.error("Rankings job failed: %s", exc)
        sys.exit(1)
    finally:
        stop_spark(spark)


if __name__ == "__main__":
    main()
