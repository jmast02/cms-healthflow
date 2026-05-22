# CMS HealthFlow — Healthcare Claims Analytics Pipeline

A production-style data engineering pipeline that processes real CMS (Centers for Medicare & Medicaid Services) healthcare claims data at scale using PySpark, exposes analytics through a FastAPI layer, and demonstrates data quality, observability, and the full modern DE stack.

---

## Project Goal

Build an end-to-end data pipeline that ingests massive, messy public healthcare datasets from CMS, processes and normalizes them at scale using PySpark, stores analytics-ready data in PostgreSQL, and serves provider comparison and cost analytics through a FastAPI REST API. The project is designed to mirror real-world healthcare data engineering and highlight skills directly relevant to healthtech, govtech, and enterprise DE roles.

---

## Why CMS Data?

CMS publishes some of the largest, most complex free datasets available:

- **Medicare Provider Utilization and Payment Data** — millions of rows of provider billing records
- **Hospital Compare** — quality metrics across thousands of hospitals
- **Medicare Part D** — prescription drug claims by provider
- **Inpatient Prospective Payment System (IPPS)** — hospital charges vs actual Medicare payments
- **Physician and Other Supplier Data** — individual provider claims across all specialties

This data is:
- **Massive** — millions of rows, warranting Spark over Pandas
- **Messy** — inconsistent formats, missing values, schema variations across years
- **Compliance-relevant** — same data governance concerns as production healthcare systems
- **Free** — no API key, no cost, download directly from data.cms.gov

---

## Architecture Overview

```
CMS Public Datasets (data.cms.gov)
        ↓  HTTP download / bulk CSV
Raw Data Lake (local filesystem / S3-compatible)
        ↓
PySpark Processing Layer
  - Schema normalization
  - Data cleaning and validation
  - Aggregations and analytics
        ↓
PostgreSQL (Data Warehouse)
  ┌── raw schema (Bronze)
  ├── staging schema (Silver)
  └── analytics schema (Gold)
        ↓
FastAPI (REST API Layer)
  - Provider search and comparison
  - Cost analytics by procedure
  - Hospital quality rankings
  - Geographic drill-downs
        ↓
Airflow (Orchestration)
  - Scheduled data refresh
  - Quality gates
  - Pipeline alerting
        ↓
Plotly Dash (Optional Dashboard)
  - Provider comparison UI
  - Cost heatmaps by geography
```

---

## Tech Stack

### Data Source
- **CMS Open Data Portal** — data.cms.gov, free public datasets, no auth required
- **Formats** — CSV, JSON, some Excel

### Big Data Processing
- **Apache Spark (PySpark)** — distributed processing for million+ row datasets
- **Delta Lake** — ACID transactions on top of Parquet files (bonus — modern DE stack)

### Transformation
- **PySpark DataFrame API** — transformations, aggregations, window functions
- **PySpark SQL** — SQL-style queries on DataFrames
- **dbt** — final transformation layer on top of PostgreSQL (reuse from DiamondPipeline)

### Storage
- **PostgreSQL** — data warehouse for normalized, analytics-ready data
- **Parquet files** — intermediate storage between Spark jobs (columnar, compressed)

### API Layer
- **FastAPI** — async Python REST API serving analytics results
- **SQLAlchemy** — ORM for PostgreSQL queries
- **Pydantic** — request/response validation

### Orchestration
- **Apache Airflow** — pipeline scheduling, quality gates, retry logic

### Infrastructure
- **Docker / Docker Compose** — containerize everything
- **MinIO** — local S3-compatible object storage for raw files (simulates AWS S3)

### Observability
- **Prometheus + Grafana** — pipeline metrics (reuse from DiamondPipeline)
- **Great Expectations** — data quality validation (new addition — major DE resume keyword)

---

## Dataset Details

### Primary Dataset — Medicare Provider Utilization (Start Here)
```
URL: https://data.cms.gov/provider-summary-by-type-of-service
Size: ~10-15 million rows
Format: CSV
Key fields:
  - NPI (provider ID)
  - Provider name, address, state, zip
  - HCPCS code (procedure code)
  - Number of services
  - Average submitted charge
  - Average Medicare payment
  - Average beneficiary age
```

### Secondary Datasets (Add as Project Grows)
```
Hospital Compare:       Hospital quality scores, readmission rates, patient satisfaction
Part D Prescribers:     Drug prescribing patterns by provider
IPPS:                   Inpatient hospital charges vs Medicare payments by DRG
Physician Compare:      Provider demographics, specialties, group affiliations
```

---

## Key Data Engineering Concepts Demonstrated

### Why PySpark Over Pandas
At 10-15 million rows, Pandas loads everything into memory — slow and memory-intensive. PySpark distributes the work across partitions, processing data in parallel. This is the core reason to use Spark:

```python
# Pandas — loads all 15M rows into RAM
df = pd.read_csv("cms_provider_data.csv")  # might crash

# PySpark — processes in distributed partitions
df = spark.read.csv("cms_provider_data.csv", header=True, inferSchema=True)
df.count()  # triggers distributed execution
```

### Window Functions in PySpark
Same concept as SQL/dbt window functions — compute rankings across groups:

```python
from pyspark.sql import Window
from pyspark.sql.functions import rank, avg, col

# Rank providers by average Medicare payment within each specialty
window = Window.partitionBy("provider_type").orderBy(col("avg_payment").desc())

df_ranked = df.withColumn("specialty_rank", rank().over(window))
```

### Incremental Processing
Don't reprocess all 15M rows every run — only new/changed records:

```python
# Read last processed timestamp
last_run = get_last_run_timestamp()

# Only process records newer than last run
new_records = df.filter(col("last_updated") > last_run)
```

### Schema Evolution
CMS changes column names between dataset versions. Handle gracefully:

```python
# Map inconsistent CMS column names to standard schema
COLUMN_MAPPING = {
    "Rndrng_Prvdr_NPI":     "provider_npi",
    "Rndrng_Prvdr_Last_Org_Name": "provider_name",
    "Tot_Srvcs":            "total_services",
    "Avg_Mdcr_Pymt_Amt":   "avg_medicare_payment",
}

def normalize_schema(df):
    for old_col, new_col in COLUMN_MAPPING.items():
        if old_col in df.columns:
            df = df.withColumnRenamed(old_col, new_col)
    return df
```

### Data Quality with Great Expectations
Industry-standard data quality tool — major resume differentiator:

```python
import great_expectations as gx

context = gx.get_context()

# Define expectations on your dataset
suite = context.add_expectation_suite("cms_provider_suite")

# Provider NPI must never be null
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="provider_npi")
)

# Average payment must be positive
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="avg_medicare_payment",
        min_value=0,
        max_value=1000000
    )
)

# State must be valid US state code
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="provider_state",
        value_set=VALID_US_STATES
    )
)

# Run validation
results = context.run_validation_operator(
    "action_list_operator",
    assets_to_validate=[batch]
)
```

---

## FastAPI Layer — Key Endpoints

```python
# main.py
from fastapi import FastAPI, Query
from typing import Optional

app = FastAPI(
    title="CMS HealthFlow API",
    description="Healthcare provider analytics from CMS public data",
    version="1.0.0"
)

# Provider search
@app.get("/api/v1/providers")
async def search_providers(
    state:    Optional[str] = Query(None, description="State code e.g. FL"),
    specialty:Optional[str] = Query(None, description="Provider specialty"),
    zip_code: Optional[str] = Query(None, description="ZIP code"),
    limit:    int = Query(50, le=500),
):
    """Search and filter providers by location and specialty."""
    ...

# Provider detail
@app.get("/api/v1/providers/{npi}")
async def get_provider(npi: str):
    """Get full analytics profile for a specific provider."""
    ...

# Procedure cost comparison
@app.get("/api/v1/procedures/{hcpcs_code}/costs")
async def procedure_costs(
    hcpcs_code: str,
    state: Optional[str] = None,
):
    """Compare Medicare payments for a procedure across providers."""
    ...

# Hospital rankings
@app.get("/api/v1/hospitals/rankings")
async def hospital_rankings(
    state:  Optional[str] = None,
    metric: str = Query("quality_score",
                        enum=["quality_score", "readmission_rate", "patient_satisfaction"])
):
    """Rank hospitals by quality metrics."""
    ...

# Geographic cost heatmap data
@app.get("/api/v1/analytics/cost-by-geography")
async def cost_by_geography(procedure_code: Optional[str] = None):
    """Average Medicare costs aggregated by state and ZIP."""
    ...

# Data freshness
@app.get("/api/v1/health")
async def health_check():
    """Pipeline health and data freshness status."""
    ...
```

---

## Folder Structure

```
cms-healthflow/
├── ingestion/
│   ├── download.py          # Download CMS datasets from data.cms.gov
│   ├── validate.py          # Great Expectations validation
│   └── ingest.py            # Load raw files to MinIO/local storage
│
├── spark/
│   ├── jobs/
│   │   ├── normalize.py     # Schema normalization job
│   │   ├── aggregate.py     # Provider-level aggregations
│   │   ├── quality.py       # Data quality scoring
│   │   └── rankings.py      # Hospital/provider rankings
│   ├── utils/
│   │   ├── schema.py        # Column mapping and type casting
│   │   └── session.py       # SparkSession factory
│   └── config.py            # Spark configuration
│
├── dbt/
│   ├── models/
│   │   ├── staging/         # Clean and type Spark outputs
│   │   └── marts/           # Business-ready analytics tables
│   └── tests/               # dbt schema tests
│
├── api/
│   ├── main.py              # FastAPI app and router
│   ├── routers/
│   │   ├── providers.py     # Provider endpoints
│   │   ├── procedures.py    # Procedure cost endpoints
│   │   ├── hospitals.py     # Hospital ranking endpoints
│   │   └── analytics.py     # Geographic analytics endpoints
│   ├── models/              # SQLAlchemy models
│   ├── schemas/             # Pydantic request/response schemas
│   └── db.py                # Database connection
│
├── airflow/
│   └── dags/
│       └── cms_pipeline.py  # Main orchestration DAG
│
├── observability/
│   ├── prometheus/
│   └── grafana/
│
├── tests/
│   ├── test_spark_jobs.py
│   ├── test_api_endpoints.py
│   └── test_data_quality.py
│
├── docker-compose.yml       # Full stack: Spark, Postgres, MinIO, Airflow, API
├── requirements.txt
├── Makefile                 # make download, make spark-run, make api-up
├── .env.example
└── README.md
```

---

## Project Milestones

### Week 1 — Data Ingestion + Raw Layer
- Download CMS Provider Utilization dataset from data.cms.gov
- Set up PostgreSQL with bronze/silver/gold schema structure
- Set up MinIO (local S3) for raw file storage
- Write ingestion script that downloads and stores raw CSVs
- Set up Docker Compose with Spark, PostgreSQL, MinIO

### Week 2 — PySpark Processing
- Set up SparkSession and PySpark environment
- Write schema normalization job — standardize column names, cast types
- Write data cleaning job — handle nulls, outliers, invalid values
- Write aggregation job — provider-level and procedure-level summaries
- Write window function job — provider rankings within specialty and geography
- Store Spark outputs as Parquet files and load to PostgreSQL silver layer

### Week 3 — Data Quality + dbt
- Set up Great Expectations — define and run expectation suites
- Build dbt staging models on top of Spark outputs
- Build dbt mart models — provider profiles, procedure costs, hospital rankings
- Add dbt schema tests (not_null, unique, accepted_values)
- Add freshness checks and volume checks

### Week 4 — FastAPI Layer
- Set up FastAPI with SQLAlchemy connecting to PostgreSQL gold layer
- Build provider search and detail endpoints
- Build procedure cost comparison endpoint
- Build hospital rankings endpoint
- Build geographic analytics endpoint
- Add Pydantic validation, error handling, rate limiting
- Add OpenAPI documentation (FastAPI does this automatically)

### Week 5 — Airflow Orchestration
- Build Airflow DAG orchestrating the full pipeline
- Add quality gate tasks (Great Expectations, volume checks)
- Add retry logic and failure alerting
- Schedule weekly refresh (CMS updates data periodically)

### Week 6 — Polish + Deploy
- Write comprehensive README with architecture diagram
- Add pytest test suite for Spark jobs and API endpoints
- Add Prometheus metrics to FastAPI and Spark jobs
- Build Grafana dashboard for pipeline monitoring
- Optional: Deploy FastAPI to Railway or Render publicly
- Clean commit history, screenshots in README

---

## Resume Bullet (When Complete)

*"Built CMS HealthFlow — a healthcare claims analytics pipeline processing 15M+ Medicare provider records using PySpark for distributed transformation, dbt for data modeling, and Great Expectations for data quality validation. Exposed analytics through a FastAPI REST API enabling provider comparison, procedure cost analysis, and hospital quality rankings. Orchestrated by Airflow with observability via Prometheus and Grafana. Full stack runs via Docker Compose."*

---

## New Resume Keywords This Project Adds

- **PySpark / Apache Spark** — distributed data processing
- **FastAPI** — modern async Python REST API
- **Great Expectations** — data quality validation
- **Delta Lake / Parquet** — modern file formats
- **MinIO / S3** — object storage
- **Healthcare data / CMS** — domain knowledge
- **Provider analytics** — healthcare-specific DE experience

## Keywords Reinforced from DiamondPipeline

- Apache Airflow
- dbt
- PostgreSQL
- Docker / Docker Compose
- Prometheus + Grafana
- Data quality testing
- Medallion architecture

---

## Why This Impresses DE Interviewers

**PySpark is warranted** — 15M rows is genuinely large enough that Spark makes sense. You're not using a sledgehammer on a nail.

**Healthcare domain** — Healthtech is one of the biggest DE hiring sectors. CMS data experience signals you understand compliance-sensitive, regulated data environments.

**Great Expectations** — Most candidates don't know this tool. It shows you think about data quality as a first-class concern, not an afterthought.

**FastAPI over Flask** — Modern, async, auto-generates OpenAPI docs. Shows you're current.

**Two projects together** — DiamondPipeline (streaming/Kafka/real-time) + CMS HealthFlow (batch/Spark/analytics API) shows you understand both streaming and batch paradigms. That's the full DE picture.

---

## Data Sources

- **CMS Provider Data**: https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-provider-and-service
- **Hospital Compare**: https://data.cms.gov/provider-data/topics/hospitals
- **Medicare Part D**: https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers
- **Great Expectations Docs**: https://docs.greatexpectations.io
- **PySpark Docs**: https://spark.apache.org/docs/latest/api/python/
- **FastAPI Docs**: https://fastapi.tiangolo.com
