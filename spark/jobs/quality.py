"""
Spark Job — Data Quality Scoring (Silver enrichment)

Reads Silver Parquet and appends a quality_score (0-100) to each row
based on completeness, validity, and consistency checks.
Writes back an enriched Silver layer.

Run:  python -m spark.jobs.quality
"""

import logging
import sys

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark.config import CONFIG
from spark.utils.session import get_spark_session, stop_spark

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

VALID_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "DC", "PR", "VI", "GU", "AS", "MP",
}


def read_silver(spark, year: int) -> DataFrame:
    path = f"{CONFIG.parquet_dir}/silver/provider/{year}"
    log.info("Reading Silver Parquet: %s", path)
    return spark.read.parquet(path)


def add_quality_score(df: DataFrame) -> DataFrame:
    """
    Score each row 0-100 based on:
      - NPI present and 10 digits        (+20)
      - provider_name not null            (+15)
      - provider_state valid US code      (+15)
      - hcpcs_code not null               (+15)
      - avg_medicare_payment > 0          (+15)
      - total_services > 0                (+10)
      - provider_zip not null             (+10)
    """
    score = F.lit(0)

    # NPI: 10-digit numeric string
    score = score + F.when(
        F.col("provider_npi").rlike(r"^\d{10}$"), 20
    ).otherwise(0)

    score = score + F.when(F.col("provider_name").isNotNull(), 15).otherwise(0)

    score = score + F.when(
        F.col("provider_state").isin(*VALID_STATES), 15
    ).otherwise(0)

    score = score + F.when(F.col("hcpcs_code").isNotNull(), 15).otherwise(0)

    score = score + F.when(
        F.col("avg_medicare_payment").isNotNull() & (F.col("avg_medicare_payment") > 0), 15
    ).otherwise(0)

    score = score + F.when(
        F.col("total_services").isNotNull() & (F.col("total_services") > 0), 10
    ).otherwise(0)

    score = score + F.when(F.col("provider_zip").isNotNull(), 10).otherwise(0)

    # Flag outliers: submitted charge > 10x Medicare payment (typical fraud signal)
    is_outlier_charge = F.when(
        F.col("avg_submitted_charge").isNotNull()
        & F.col("avg_medicare_payment").isNotNull()
        & (F.col("avg_medicare_payment") > 0)
        & (F.col("avg_submitted_charge") > F.col("avg_medicare_payment") * 10),
        True,
    ).otherwise(False)

    # Flag outliers: avg payment > $10,000 (statistical outlier for most specialties)
    is_outlier_payment = F.when(
        F.col("avg_medicare_payment") > 10_000, True
    ).otherwise(False)

    return (
        df
        .withColumn("quality_score", score.cast("decimal(5,2)"))
        .withColumn("is_outlier_charge", is_outlier_charge)
        .withColumn("is_outlier_payment", is_outlier_payment)
    )


def log_quality_summary(df: DataFrame) -> None:
    summary = df.agg(
        F.avg("quality_score").alias("avg_score"),
        F.min("quality_score").alias("min_score"),
        F.count(F.when(F.col("quality_score") >= 80, 1)).alias("high_quality"),
        F.count(F.when(F.col("quality_score") < 50, 1)).alias("low_quality"),
        F.count(F.when(F.col("is_outlier_charge"), 1)).alias("outlier_charge_count"),
        F.count(F.when(F.col("is_outlier_payment"), 1)).alias("outlier_payment_count"),
        F.count("*").alias("total_rows"),
    ).collect()[0]

    log.info(
        "Quality summary — avg=%.1f  min=%.0f  high_quality=%d (%.1f%%)  low_quality=%d  "
        "outlier_charge=%d  outlier_payment=%d",
        summary["avg_score"],
        summary["min_score"],
        summary["high_quality"],
        summary["high_quality"] / summary["total_rows"] * 100,
        summary["low_quality"],
        summary["outlier_charge_count"],
        summary["outlier_payment_count"],
    )


def main() -> None:
    spark = get_spark_session("cms-quality")
    try:
        year = CONFIG.dataset_year
        df = read_silver(spark, year)
        df = add_quality_score(df)

        log_quality_summary(df)

        out_path = f"{CONFIG.parquet_dir}/silver/provider_scored/{year}"
        log.info("Writing scored Silver Parquet → %s", out_path)
        df.write.mode("overwrite").parquet(out_path)

        log.info("Quality scoring job complete.")
    except Exception as exc:
        log.error("Quality job failed: %s", exc)
        sys.exit(1)
    finally:
        stop_spark(spark)


if __name__ == "__main__":
    main()
