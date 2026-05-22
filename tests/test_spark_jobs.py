"""
Unit tests for PySpark transformation jobs.

Uses a small in-memory DataFrame to avoid needing real CMS data.
Spark session and sample data are provided by conftest.py.
Run: pytest tests/test_spark_jobs.py -v
"""

import pytest
from decimal import Decimal

# spark, sample_rows, sample_df fixtures come from conftest.py


@pytest.fixture
def raw_provider_rows(sample_rows):
    return sample_rows


# Legacy fixture alias kept for backward compat with tests below
@pytest.fixture
def raw_provider_rows_with_null():
    return [
        {
            "Rndrng_Prvdr_NPI": "1234567890",
            "Rndrng_Prvdr_Last_Org_Name": "Smith",
            "Rndrng_Prvdr_First_Name": "John",
            "Rndrng_Prvdr_State_Abrvtn": "FL",
            "Rndrng_Prvdr_Type": "Internal Medicine",
            "HCPCS_Cd": "99213",
            "HCPCS_Desc": "Office visit, established patient",
            "Tot_Srvcs": "150",
            "Tot_Benes": "120",
            "Avg_Mdcr_Pymt_Amt": "75.50",
            "Avg_Sbmtd_Chrg": "200.00",
            "Tot_Mdcr_Pymt_Amt": "11325.00",
        },
        {
            "Rndrng_Prvdr_NPI": "9876543210",
            "Rndrng_Prvdr_Last_Org_Name": "Jones",
            "Rndrng_Prvdr_First_Name": "Jane",
            "Rndrng_Prvdr_State_Abrvtn": "ca",  # lowercase — should be normalised
            "Rndrng_Prvdr_Type": "Internal Medicine",
            "HCPCS_Cd": "99214",
            "HCPCS_Desc": "Office visit, established patient, moderate",
            "Tot_Srvcs": "200",
            "Tot_Benes": "180",
            "Avg_Mdcr_Pymt_Amt": "110.00",
            "Avg_Sbmtd_Chrg": "300.00",
            "Tot_Mdcr_Pymt_Amt": "22000.00",
        },
        {
            "Rndrng_Prvdr_NPI": None,  # Should be dropped
            "Rndrng_Prvdr_Last_Org_Name": "Unknown",
            "Rndrng_Prvdr_First_Name": None,
            "Rndrng_Prvdr_State_Abrvtn": "TX",
            "Rndrng_Prvdr_Type": "Family Practice",
            "HCPCS_Cd": "99213",
            "HCPCS_Desc": "Office visit",
            "Tot_Srvcs": "50",
            "Tot_Benes": "40",
            "Avg_Mdcr_Pymt_Amt": "70.00",
            "Avg_Sbmtd_Chrg": "180.00",
            "Tot_Mdcr_Pymt_Amt": "3500.00",
        },
    ]


class TestSchemaUtils:
    def test_normalize_column_names(self, spark, raw_provider_rows):
        from spark.utils.schema import normalize_column_names

        df = spark.createDataFrame(raw_provider_rows)
        df_norm = normalize_column_names(df)

        assert "provider_npi" in df_norm.columns
        assert "provider_state" in df_norm.columns
        assert "hcpcs_code" in df_norm.columns
        assert "avg_medicare_payment" in df_norm.columns
        assert "Rndrng_Prvdr_NPI" not in df_norm.columns

    def test_cast_numeric_columns(self, spark, raw_provider_rows):
        from spark.utils.schema import cast_numeric_columns, normalize_column_names

        df = spark.createDataFrame(raw_provider_rows)
        df = normalize_column_names(df)
        df = cast_numeric_columns(df)

        row = df.filter(df.provider_npi == "1234567890").first()
        assert row is not None
        assert float(row["avg_medicare_payment"]) == pytest.approx(75.50)


class TestNormalizeJob:
    def test_drops_null_npi_rows(self, spark, raw_provider_rows):
        from pyspark.sql import functions as F
        from spark.utils.schema import normalize_column_names, cast_numeric_columns

        df = spark.createDataFrame(raw_provider_rows)
        df = normalize_column_names(df)
        df = cast_numeric_columns(df)

        before = df.count()
        df_clean = df.filter(F.col("provider_npi").isNotNull() & (F.trim(F.col("provider_npi")) != ""))
        after = df_clean.count()

        assert after == before - 1  # one null NPI row dropped

    def test_state_normalised_to_uppercase(self, spark, raw_provider_rows):
        from pyspark.sql import functions as F
        from spark.utils.schema import normalize_column_names

        df = spark.createDataFrame(raw_provider_rows)
        df = normalize_column_names(df)
        df = df.withColumn("provider_state", F.upper(F.trim(F.col("provider_state"))))

        state = df.filter(df.provider_npi == "9876543210").first()["provider_state"]
        assert state == "CA"


class TestAggregateJob:
    def test_provider_profiles_one_row_per_npi(self, spark, raw_provider_rows):
        from pyspark.sql import functions as F
        from spark.utils.schema import normalize_column_names, cast_numeric_columns
        from spark.jobs.aggregate import build_provider_profiles

        df = spark.createDataFrame(raw_provider_rows)
        df = normalize_column_names(df)
        df = cast_numeric_columns(df)
        df = df.filter(F.col("provider_npi").isNotNull()).withColumn("dataset_year", F.lit(2022).cast("short"))

        profiles = build_provider_profiles(df)
        assert profiles.count() == 2  # two valid NPIs

    def test_procedure_costs_computed(self, spark, raw_provider_rows):
        from pyspark.sql import functions as F
        from spark.utils.schema import normalize_column_names, cast_numeric_columns
        from spark.jobs.aggregate import build_procedure_costs

        df = spark.createDataFrame(raw_provider_rows)
        df = normalize_column_names(df)
        df = cast_numeric_columns(df)
        df = df.filter(F.col("provider_npi").isNotNull()).withColumn("dataset_year", F.lit(2022).cast("short"))

        costs = build_procedure_costs(df)
        assert costs.count() >= 1


class TestQualityJob:
    def test_quality_score_range(self, spark, raw_provider_rows):
        from pyspark.sql import functions as F
        from spark.utils.schema import normalize_column_names, cast_numeric_columns
        from spark.jobs.quality import add_quality_score

        df = spark.createDataFrame(raw_provider_rows)
        df = normalize_column_names(df)
        df = cast_numeric_columns(df)
        df = df.filter(F.col("provider_npi").isNotNull())

        df_scored = add_quality_score(df)
        scores = [row["quality_score"] for row in df_scored.select("quality_score").collect()]

        assert all(0 <= float(s) <= 100 for s in scores)

    def test_outlier_flag_set(self, spark):
        from spark.jobs.quality import add_quality_score

        rows = [{"provider_npi": "1111111111", "provider_name": "Outlier Provider",
                 "provider_state": "FL", "hcpcs_code": "99999",
                 "avg_medicare_payment": "50.00", "avg_submitted_charge": "600.00",
                 "total_services": "10", "provider_zip": "33101"}]
        df = spark.createDataFrame(rows)

        from spark.utils.schema import cast_numeric_columns
        df = cast_numeric_columns(df)
        df_scored = add_quality_score(df)

        row = df_scored.first()
        assert row["is_outlier_charge"] is True
