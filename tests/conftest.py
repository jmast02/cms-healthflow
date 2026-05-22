"""
Shared pytest fixtures for the CMS HealthFlow test suite.

Spark session is scoped to the test session so it starts once
and is reused across all Spark tests — avoids the 10+ second
JVM startup penalty on every test file.
"""

import os
import tempfile
from pathlib import Path
from typing import Generator
from unittest.mock import MagicMock

import pandas as pd
import pytest

# ── Environment defaults for tests ───────────────────────────────────────
os.environ.setdefault("POSTGRES_HOST", "localhost")
os.environ.setdefault("POSTGRES_USER", "healthflow")
os.environ.setdefault("POSTGRES_PASSWORD", "healthflow_secret")
os.environ.setdefault("POSTGRES_DB", "cms_healthflow")
os.environ.setdefault("SPARK_MASTER", "local[1]")


# ── Spark ─────────────────────────────────────────────────────────────────
@pytest.fixture(scope="session")
def spark():
    """Single SparkSession shared across the entire test run."""
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("cms-healthflow-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ── Sample data ───────────────────────────────────────────────────────────
SAMPLE_ROWS = [
    {
        "Rndrng_Prvdr_NPI": "1234567890",
        "Rndrng_Prvdr_Last_Org_Name": "Smith",
        "Rndrng_Prvdr_First_Name": "John",
        "Rndrng_Prvdr_State_Abrvtn": "FL",
        "Rndrng_Prvdr_City": "Miami",
        "Rndrng_Prvdr_Zip5": "33101",
        "Rndrng_Prvdr_Type": "Internal Medicine",
        "Rndrng_Prvdr_Gndr": "M",
        "Rndrng_Prvdr_Mdcr_Prtcptg_Ind": "Y",
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
        "Rndrng_Prvdr_State_Abrvtn": "CA",
        "Rndrng_Prvdr_City": "Los Angeles",
        "Rndrng_Prvdr_Zip5": "90001",
        "Rndrng_Prvdr_Type": "Cardiology",
        "Rndrng_Prvdr_Gndr": "F",
        "Rndrng_Prvdr_Mdcr_Prtcptg_Ind": "Y",
        "HCPCS_Cd": "93000",
        "HCPCS_Desc": "Electrocardiogram, routine ECG",
        "Tot_Srvcs": "500",
        "Tot_Benes": "400",
        "Avg_Mdcr_Pymt_Amt": "18.75",
        "Avg_Sbmtd_Chrg": "45.00",
        "Tot_Mdcr_Pymt_Amt": "9375.00",
    },
    {
        "Rndrng_Prvdr_NPI": "5555555555",
        "Rndrng_Prvdr_Last_Org_Name": "Regional Medical Center",
        "Rndrng_Prvdr_First_Name": None,
        "Rndrng_Prvdr_State_Abrvtn": "TX",
        "Rndrng_Prvdr_City": "Houston",
        "Rndrng_Prvdr_Zip5": "77001",
        "Rndrng_Prvdr_Type": "Internal Medicine",
        "Rndrng_Prvdr_Gndr": None,
        "Rndrng_Prvdr_Mdcr_Prtcptg_Ind": "Y",
        "HCPCS_Cd": "99214",
        "HCPCS_Desc": "Office visit, established patient, moderate complexity",
        "Tot_Srvcs": "300",
        "Tot_Benes": "280",
        "Avg_Mdcr_Pymt_Amt": "110.25",
        "Avg_Sbmtd_Chrg": "275.00",
        "Tot_Mdcr_Pymt_Amt": "33075.00",
    },
]


@pytest.fixture
def sample_rows() -> list[dict]:
    return [r.copy() for r in SAMPLE_ROWS]


@pytest.fixture
def sample_df(spark, sample_rows):
    """Normalised + cast Spark DataFrame from sample rows."""
    from spark.utils.schema import cast_numeric_columns, normalize_column_names
    from pyspark.sql import functions as F

    df = spark.createDataFrame(sample_rows)
    df = normalize_column_names(df)
    df = cast_numeric_columns(df)
    df = df.filter(F.col("provider_npi").isNotNull())
    df = df.withColumn("dataset_year", F.lit(2022).cast("short"))
    return df


@pytest.fixture
def sample_csv(sample_rows) -> Generator[Path, None, None]:
    """Write sample rows to a temp CSV file."""
    df = pd.DataFrame(sample_rows)
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
        df.to_csv(f.name, index=False)
        yield Path(f.name)
    Path(f.name).unlink(missing_ok=True)


# ── FastAPI test client ───────────────────────────────────────────────────
@pytest.fixture(scope="module")
def api_client():
    from fastapi.testclient import TestClient
    from api.main import app

    with TestClient(app) as c:
        yield c


# ── Mock DB session ───────────────────────────────────────────────────────
@pytest.fixture
def mock_db():
    db = MagicMock()
    db.execute.return_value.scalars.return_value.all.return_value = []
    db.get.return_value = None
    return db
