"""
Spark Job — Aggregations (Silver → Gold)

Reads normalized Silver Parquet and computes:
  - Provider-level profiles (total spend, unique procedures, avg payments)
  - Procedure-level cost analytics (avg/min/max/stddev by state)
  - Geographic cost rollups (avg payment by state and ZIP)

Run:  python -m spark.jobs.aggregate
"""

import logging
import sys

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark.config import CONFIG
from spark.utils.pipeline_log import finish_run, start_run
from spark.utils.session import get_spark_session, stop_spark

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def read_silver(spark, year: int) -> DataFrame:
    path = f"{CONFIG.parquet_dir}/silver/provider/{year}"
    log.info("Reading Silver Parquet: %s", path)
    return spark.read.parquet(path)


def build_provider_profiles(df: DataFrame) -> DataFrame:
    """Aggregate to one row per provider NPI."""
    return df.groupBy(
        "provider_npi",
        "provider_name",
        "provider_first_name",
        "provider_type",
        "provider_state",
        "provider_city",
        "provider_zip",
        "provider_gender",
        "medicare_participation",
        "dataset_year",
    ).agg(
        F.count("hcpcs_code").alias("total_procedures"),
        F.sum("total_beneficiaries").alias("total_beneficiaries"),
        F.sum("total_services").alias("total_services"),
        F.sum("total_medicare_payment").alias("total_medicare_payment"),
        F.avg("avg_medicare_payment").alias("avg_medicare_payment"),
        F.avg("avg_submitted_charge").alias("avg_submitted_charge"),
        F.countDistinct("hcpcs_code").alias("unique_hcpcs_codes"),
    )


def build_procedure_costs(df: DataFrame) -> DataFrame:
    """Per-procedure cost analytics, broken down by state."""
    return df.groupBy("hcpcs_code", "hcpcs_description", "provider_state", "dataset_year").agg(
        F.countDistinct("provider_npi").alias("provider_count"),
        F.sum("total_services").alias("total_services"),
        F.avg("avg_submitted_charge").alias("avg_submitted_charge"),
        F.avg("avg_medicare_payment").alias("avg_medicare_payment"),
        F.percentile_approx("avg_medicare_payment", 0.5).alias("median_medicare_payment"),
        F.min("avg_medicare_payment").alias("min_medicare_payment"),
        F.max("avg_medicare_payment").alias("max_medicare_payment"),
        F.stddev("avg_medicare_payment").alias("stddev_medicare_payment"),
    )


def build_cost_by_geography(df: DataFrame) -> DataFrame:
    """Average Medicare payment and charge rolled up to state + ZIP."""
    return df.groupBy("provider_state", "provider_zip", "dataset_year").agg(
        F.countDistinct("provider_npi").alias("total_providers"),
        F.sum("total_services").alias("total_services"),
        F.avg("avg_medicare_payment").alias("avg_medicare_payment"),
        F.avg("avg_submitted_charge").alias("avg_submitted_charge"),
    )


def write_gold(df: DataFrame, name: str, year: int) -> None:
    out_path = f"{CONFIG.parquet_dir}/gold/{name}/{year}"
    log.info("Writing Gold Parquet → %s", out_path)
    df.write.mode("overwrite").parquet(out_path)


def write_postgres_gold(df: DataFrame, table: str) -> None:
    log.info("Writing to PostgreSQL gold.%s", table)
    (
        df.write
        .format("jdbc")
        .option("url", CONFIG.jdbc_url)
        .option("dbtable", f"gold.{table}")
        .option("user", CONFIG.postgres_user)
        .option("password", CONFIG.postgres_password)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )


def main() -> None:
    spark = get_spark_session("cms-aggregate")
    run_id = start_run("aggregate", f"provider/{CONFIG.dataset_year}")
    total_rows = 0
    error_msg = None

    try:
        year = CONFIG.dataset_year
        df_silver = read_silver(spark, year)
        df_silver.cache()

        log.info("Building provider profiles...")
        df_providers = build_provider_profiles(df_silver)
        write_gold(df_providers, "provider_profiles", year)
        provider_count = df_providers.count()
        log.info("Provider profiles: %d rows", provider_count)
        total_rows += provider_count

        log.info("Building procedure costs...")
        df_procedures = build_procedure_costs(df_silver)
        write_gold(df_procedures, "procedure_costs", year)
        proc_count = df_procedures.count()
        log.info("Procedure costs: %d rows", proc_count)
        total_rows += proc_count

        log.info("Building geographic costs...")
        df_geo = build_cost_by_geography(df_silver)
        write_gold(df_geo, "cost_by_geography", year)
        geo_count = df_geo.count()
        log.info("Geographic costs: %d rows", geo_count)
        total_rows += geo_count

        df_silver.unpersist()
        log.info("Aggregate job complete. Total output rows: %d", total_rows)
    except Exception as exc:
        error_msg = str(exc)
        log.error("Aggregate job failed: %s", exc)
        sys.exit(1)
    finally:
        finish_run(run_id, total_rows, error_message=error_msg)
        stop_spark(spark)


if __name__ == "__main__":
    main()
