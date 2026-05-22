"""
Spark Job — Schema Normalization (Bronze → Silver)

Reads raw CMS CSV from data/raw/, normalizes column names,
casts types, cleans nulls, and writes Silver Parquet + PostgreSQL bronze table.

Run:  python -m spark.jobs.normalize
"""

import logging
import sys
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark.config import CONFIG
from spark.utils.pipeline_log import finish_run, start_run
from spark.utils.schema import (
    cast_numeric_columns,
    normalize_column_names,
)
from spark.utils.session import get_spark_session, stop_spark

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def read_raw_csv(spark, year: int) -> DataFrame:
    # Prefer the full dataset; fall back to the synthetic sample for dev/demo
    candidates = [
        f"{CONFIG.raw_data_dir}/provider/cms_provider_{year}.csv",
        f"{CONFIG.raw_data_dir}/provider/cms_provider_{year}_sample.csv",
    ]
    path = next((p for p in candidates if Path(p).exists()), None)
    if path is None:
        raise FileNotFoundError(
            f"No CMS CSV found for year {year} in {CONFIG.raw_data_dir}/provider/. "
            "Run 'make generate-sample' or 'make download'."
        )

    log.info("Reading raw CSV: %s", path)
    df = spark.read.csv(path, header=True, inferSchema=False)
    log.info("Raw schema: %d columns, loading...", len(df.columns))
    return df


def clean(df: DataFrame, year: int) -> DataFrame:
    df = normalize_column_names(df)
    df = cast_numeric_columns(df)

    # Tag every row with the source dataset year
    df = df.withColumn("dataset_year", F.lit(year).cast("short"))

    # Drop rows with no NPI — unusable without a provider identifier
    before = df.count()
    df = df.filter(F.col("provider_npi").isNotNull() & (F.trim(F.col("provider_npi")) != ""))
    dropped = before - df.count()
    if dropped:
        log.warning("Dropped %d rows with null/empty NPI", dropped)

    # Standardise state codes to uppercase
    if "provider_state" in df.columns:
        df = df.withColumn("provider_state", F.upper(F.trim(F.col("provider_state"))))

    # Strip whitespace from string columns
    string_cols = [f.name for f in df.schema.fields if str(f.dataType) == "StringType()"]
    for col_name in string_cols:
        df = df.withColumn(col_name, F.trim(F.col(col_name)))

    # Replace empty strings with NULL (CMS uses "" for missing values)
    for col_name in string_cols:
        df = df.withColumn(
            col_name,
            F.when(F.col(col_name) == "", None).otherwise(F.col(col_name)),
        )

    return df


def write_parquet(df: DataFrame, year: int) -> None:
    out_path = f"{CONFIG.parquet_dir}/silver/provider/{year}"
    log.info("Writing Silver Parquet → %s", out_path)
    df.write.mode("overwrite").parquet(out_path)
    log.info("Parquet write complete")


def write_postgres_bronze(df: DataFrame) -> None:
    log.info("Writing to PostgreSQL bronze.provider_claims")
    (
        df.write
        .format("jdbc")
        .option("url", CONFIG.jdbc_url)
        .option("dbtable", "bronze.provider_claims")
        .option("user", CONFIG.postgres_user)
        .option("password", CONFIG.postgres_password)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )
    log.info("PostgreSQL write complete")


def main() -> None:
    spark = get_spark_session("cms-normalize")
    run_id = start_run("normalize", f"provider/{CONFIG.dataset_year}")
    row_count = 0
    error_msg = None

    try:
        year = CONFIG.dataset_year
        df_raw = read_raw_csv(spark, year)
        df_clean = clean(df_raw, year)

        row_count = df_clean.count()
        log.info("Normalized %d rows for year %d", row_count, year)

        write_parquet(df_clean, year)
        # write_postgres_bronze(df_clean)  # uncomment when PostgreSQL is running

        log.info("Normalize job complete.")
    except Exception as exc:
        error_msg = str(exc)
        log.error("Normalize job failed: %s", exc)
        sys.exit(1)
    finally:
        finish_run(run_id, row_count, error_message=error_msg)
        stop_spark(spark)


if __name__ == "__main__":
    main()
