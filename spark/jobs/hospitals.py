"""
Spark Job — Hospital Compare Processing (Bronze → Gold)

Reads the CMS Hospital General Information CSV and produces a
gold.hospital_rankings table with quality scores and state/national rankings.

Run:  python -m spark.jobs.hospitals
"""

import logging
import sys
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, ShortType, StringType
from pyspark.sql.window import Window

from spark.config import CONFIG
from spark.utils.session import get_spark_session, stop_spark

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# CMS Hospital Compare column names → canonical names
HOSPITAL_COLUMN_MAPPING = {
    "Facility ID":                       "facility_id",
    "Facility Name":                     "facility_name",
    "Address":                           "address",
    "City/Town":                         "city",
    "State":                             "state",
    "ZIP Code":                          "zip_code",
    "County/Parish":                     "county_name",
    "Phone Number":                      "phone_number",
    "Hospital Type":                     "hospital_type",
    "Hospital Ownership":                "hospital_ownership",
    "Emergency Services":                "emergency_services_raw",
    "Hospital overall rating":           "overall_rating_raw",
    "Readmission national comparison":   "readmission_national",
    "Mortality national comparison":     "mortality_national",
    "Safety of care national comparison":"safety_national",
    "Patient experience national comparison": "patient_experience",
    "Effectiveness of care national comparison": "effectiveness_national",
    "Timeliness of care national comparison": "timeliness_national",
    "Efficient use of medical imaging national comparison": "efficient_imaging",
}


def read_hospital_csv(spark, year: int) -> DataFrame:
    path = f"{CONFIG.raw_data_dir}/hospital_compare/cms_hospital_compare_{year}.csv"
    if not Path(path).exists():
        raise FileNotFoundError(
            f"Hospital Compare file not found: {path}. "
            "Run 'make download' with --dataset hospital_compare first."
        )
    log.info("Reading Hospital Compare CSV: %s", path)
    return spark.read.csv(path, header=True, inferSchema=False)


def normalize_hospital_schema(df: DataFrame) -> DataFrame:
    for raw, canonical in HOSPITAL_COLUMN_MAPPING.items():
        if raw in df.columns:
            df = df.withColumnRenamed(raw, canonical)
    return df


def clean_hospital_data(df: DataFrame, year: int) -> DataFrame:
    # Convert "Yes"/"No" to boolean
    df = df.withColumn(
        "emergency_services",
        F.when(F.upper(F.col("emergency_services_raw")) == "YES", True)
         .when(F.upper(F.col("emergency_services_raw")) == "NO", False)
         .otherwise(None)
         .cast(BooleanType()),
    ).drop("emergency_services_raw")

    # Overall rating: CMS uses "Not Available" for unrated hospitals
    df = df.withColumn(
        "overall_rating",
        F.when(
            F.col("overall_rating_raw").isin("Not Available", "N/A", ""),
            None,
        ).otherwise(F.col("overall_rating_raw").cast(ShortType())),
    ).drop("overall_rating_raw")

    # Standardise state codes
    df = df.withColumn("state", F.upper(F.trim(F.col("state"))))

    # Strip whitespace from all string columns
    str_cols = [f.name for f in df.schema.fields if str(f.dataType) == "StringType()"]
    for col in str_cols:
        df = df.withColumn(col, F.trim(F.col(col)))
        df = df.withColumn(col, F.when(F.col(col) == "", None).otherwise(F.col(col)))

    df = df.withColumn("dataset_year", F.lit(year).cast(ShortType()))
    return df


def add_hospital_rankings(df: DataFrame) -> DataFrame:
    """Rank hospitals within state and nationally by overall_rating (then by name for ties)."""
    state_window = Window.partitionBy("state").orderBy(
        F.col("overall_rating").desc_nulls_last(), "facility_name"
    )
    national_window = Window.orderBy(
        F.col("overall_rating").desc_nulls_last(), "facility_name"
    )

    return (
        df
        .withColumn("state_rank", F.rank().over(state_window))
        .withColumn("national_rank", F.rank().over(national_window))
    )


def write_gold(df: DataFrame, year: int) -> None:
    out_path = f"{CONFIG.parquet_dir}/gold/hospital_rankings/{year}"
    log.info("Writing Gold Parquet → %s", out_path)
    df.write.mode("overwrite").parquet(out_path)


def main() -> None:
    spark = get_spark_session("cms-hospitals")
    try:
        year = 2023  # Hospital Compare uses a different year cadence than provider data
        df = read_hospital_csv(spark, year)
        df = normalize_hospital_schema(df)
        df = clean_hospital_data(df, year)
        df = add_hospital_rankings(df)

        total = df.count()
        rated = df.filter(F.col("overall_rating").isNotNull()).count()
        log.info(
            "Hospital Compare processed: %d facilities, %d rated (%.0f%%)",
            total, rated, rated / total * 100 if total else 0,
        )

        write_gold(df, year)
        log.info("Hospitals job complete.")
    except FileNotFoundError as exc:
        log.warning("%s — skipping hospital job.", exc)
    except Exception as exc:
        log.error("Hospitals job failed: %s", exc)
        sys.exit(1)
    finally:
        stop_spark(spark)


if __name__ == "__main__":
    main()
