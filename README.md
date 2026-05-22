# CMS HealthFlow

**End-to-end healthcare claims analytics pipeline** processing 15M+ Medicare provider records from CMS public data using PySpark, dbt, Great Expectations, and FastAPI.

---

## Architecture

```
CMS Open Data Portal (data.cms.gov)
        │  HTTP download / bulk CSV
        ▼
┌─────────────────────────────┐
│    Raw Data Lake (MinIO)    │   s3://cms-raw/provider/2022/
│    Local S3-compatible      │
└────────────┬────────────────┘
             │
             ▼
┌─────────────────────────────────────────────┐
│           PySpark Processing                │
│                                             │
│  normalize.py  →  quality.py               │
│  aggregate.py  →  rankings.py              │
│                                             │
│  Bronze → Silver → Gold (Parquet/Delta)    │
└────────────┬────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────┐
│         PostgreSQL Data Warehouse           │
│                                             │
│  bronze.*   raw ingested claims             │
│  silver.*   cleaned + quality-scored        │
│  gold.*     aggregated + ranked             │
└────────────┬────────────────────────────────┘
             │
       ┌─────┴──────┐
       ▼            ▼
┌────────────┐  ┌──────────────────────────────┐
│    dbt     │  │         FastAPI               │
│  staging   │  │  /api/v1/providers            │
│   marts    │  │  /api/v1/procedures/{code}    │
│   tests    │  │  /api/v1/hospitals/rankings   │
└────────────┘  │  /api/v1/analytics/           │
                └──────────────────────────────┘
                             │
                             ▼
                ┌────────────────────────┐
                │  Apache Airflow        │
                │  Weekly orchestration  │
                │  Quality gates         │
                └────────────┬───────────┘
                             │
                             ▼
                ┌────────────────────────┐
                │  Prometheus + Grafana  │
                │  Pipeline observability│
                └────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data source | CMS Open Data Portal — free, no auth |
| Big data processing | Apache Spark 3.5 (PySpark) |
| File format | Parquet + Delta Lake |
| Object storage | MinIO (local S3-compatible) |
| Data warehouse | PostgreSQL 15 (Bronze/Silver/Gold) |
| Transformations | PySpark DataFrame API + dbt |
| Data quality | Great Expectations 0.18 |
| API | FastAPI + SQLAlchemy + Pydantic |
| Orchestration | Apache Airflow 2.9 |
| Observability | Prometheus + Grafana |
| Infrastructure | Docker Compose |

---

## Datasets

| Dataset | Rows | Description |
|---|---|---|
| Medicare Physician & Suppliers | ~15M | Primary — provider billing by NPI and HCPCS code |
| Hospital Compare | ~5K hospitals | Quality scores, readmission rates, patient satisfaction |
| Medicare Part D | ~25M | Drug prescribing by provider |
| IPPS | ~160K | Inpatient charges vs Medicare payments by DRG |

---

## Quick Start

### Prerequisites
- Docker Desktop
- Python 3.11+

### 1. Clone and configure

```bash
git clone <repo>
cd cms-healthflow
cp .env.example .env
```

### 2. Start the full stack

```bash
make up
```

Services started:
| Service | URL |
|---|---|
| FastAPI docs | http://localhost:8000/docs |
| Spark Master UI | http://localhost:8080 |
| MinIO console | http://localhost:9001 (minioadmin / minioadmin) |
| Airflow | http://localhost:8090 |
| Grafana | http://localhost:3000 (admin / admin) |
| Prometheus | http://localhost:9090 |

### 3. Download CMS data

```bash
make download
# Downloads cms_provider_2022.csv (~2 GB) to data/raw/provider/
```

To generate a small synthetic sample for local testing without downloading:
```bash
python scripts/generate_sample_data.py
```

### 4. Run the pipeline

```bash
make ingest           # upload raw CSV to MinIO
make spark-normalize  # Bronze → Silver Parquet
make spark-quality    # quality scoring
make spark-aggregate  # Silver → Gold aggregations
make spark-rank       # window function rankings
make dbt-run          # Gold → PostgreSQL mart tables
make dbt-test         # validate schema tests
```

Or run everything at once:
```bash
make pipeline
```

### 5. Hit the API

```bash
# Search providers in Florida
curl "http://localhost:8000/api/v1/providers?state=FL&limit=5"

# Procedure cost comparison across states
curl "http://localhost:8000/api/v1/procedures/99213/costs"

# Top hospitals in Texas
curl "http://localhost:8000/api/v1/hospitals/rankings?state=TX"

# Geographic cost heatmap data
curl "http://localhost:8000/api/v1/analytics/cost-by-geography?state=CA"
```

---

## API Reference

Full OpenAPI docs available at **http://localhost:8000/docs**

### Key Endpoints

```
GET /api/v1/providers
    ?state=FL&specialty=cardiology&zip_code=33101
    ?min_payment=100&max_payment=500
    ?limit=50&offset=0

GET /api/v1/providers/{npi}
    Full provider analytics profile

GET /api/v1/providers/state/{state}/top
    Top-ranked providers in a state

GET /api/v1/procedures/{hcpcs_code}/costs
    Payment comparison across states for a procedure

GET /api/v1/procedures
    ?q=office+visit&state=FL

GET /api/v1/hospitals/rankings
    ?state=TX&metric=avg_payment

GET /api/v1/analytics/cost-by-geography
    ?state=CA&min_providers=10

GET /api/v1/health
    Pipeline health and data freshness

GET /metrics
    Prometheus scrape endpoint
```

---

## Project Structure

```
cms-healthflow/
├── ingestion/
│   ├── download.py          # Download CMS CSVs (progress bar, skip if cached)
│   ├── ingest.py            # Upload raw files to MinIO
│   └── validate.py          # Great Expectations validation suite
│
├── spark/
│   ├── jobs/
│   │   ├── normalize.py     # Bronze: schema normalisation + type casting
│   │   ├── quality.py       # Silver: 0-100 quality score + outlier flags
│   │   ├── aggregate.py     # Gold: provider profiles, procedure costs, geo rollups
│   │   ├── rankings.py      # Gold: window function specialty/state rankings
│   │   └── hospitals.py     # Gold: hospital quality from Hospital Compare
│   └── utils/
│       ├── schema.py        # CMS column mapping (handles year-to-year renames)
│       └── session.py       # SparkSession factory (Delta Lake, MinIO/S3A, AQE)
│
├── dbt/
│   ├── models/staging/      # stg_providers — clean bronze → silver view
│   └── models/marts/        # provider_profiles, procedure_costs (Gold tables)
│
├── api/
│   ├── main.py              # FastAPI app (Prometheus instrumentation, CORS)
│   ├── db.py                # SQLAlchemy engine + session dep
│   ├── routers/             # providers, procedures, hospitals, analytics
│   ├── models/              # SQLAlchemy ORM → gold.* tables
│   └── schemas/             # Pydantic request/response validation
│
├── airflow/dags/
│   └── cms_pipeline.py      # 10-step orchestration DAG (weekly schedule)
│
├── gx/                      # Great Expectations context
├── observability/
│   ├── prometheus/
│   └── grafana/
│
├── scripts/
│   ├── generate_sample_data.py   # Synthetic CMS CSV for local dev/testing
│   └── reset_db.py               # Drop and recreate all schemas
│
├── sql/
│   ├── 01_init_schemas.sql  # bronze/silver/gold schemas
│   └── 02_create_tables.sql # all DDL with indexes
│
├── tests/
│   ├── conftest.py          # Shared Spark + DB fixtures
│   ├── test_spark_jobs.py
│   ├── test_api_endpoints.py
│   └── test_data_quality.py
│
├── docker-compose.yml
├── Dockerfile
├── Makefile                 # make up / download / pipeline / api-dev / test
├── requirements.txt
└── pyproject.toml
```

---

## Data Engineering Concepts Demonstrated

### PySpark window functions
```python
# Rank providers by avg Medicare payment within their specialty
window = Window.partitionBy("provider_type").orderBy(col("avg_medicare_payment").desc())
df = df.withColumn("specialty_rank", rank().over(window))
```

### Medallion architecture
- **Bronze** — raw CMS data, minimal transformation, append-only
- **Silver** — cleaned, validated, quality-scored
- **Gold** — aggregated, ranked, analytics-ready; served by FastAPI

### Schema evolution handling
CMS renames columns between dataset years. `spark/utils/schema.py` maps all known variants to canonical internal names — adding support for a new year only requires extending `COLUMN_MAPPING`.

### Great Expectations data quality
- NPI completeness (never null, always 10-digit)
- State code validity (accepted US state set)
- Payment range validation (non-negative, max $1M)
- HCPCS code format regex
- Freshness checks (warn after 14 days, error after 30)

### Incremental-ready design
Pipeline metadata table (`public.pipeline_runs`) tracks every job's execution — the foundation for incremental processing where only new/updated records are reprocessed.

---

## Running Tests

```bash
# Full test suite
make test

# With coverage
make test-cov

# Individual modules
pytest tests/test_spark_jobs.py -v
pytest tests/test_api_endpoints.py -v
pytest tests/test_data_quality.py -v
```

Tests use a local PySpark session (no cluster needed) and mock DB dependencies — no external services required.

---

## Makefile Reference

```
make up              Start full Docker stack
make down            Stop all containers
make logs            Tail container logs

make download        Download CMS provider dataset (~2 GB)
make ingest          Upload raw CSV to MinIO

make spark-normalize Bronze → Silver (normalize + clean)
make spark-quality   Silver: quality scoring
make spark-aggregate Silver → Gold (aggregations)
make spark-rank      Gold: rankings
make pipeline        Run all Spark jobs in sequence

make api-dev         FastAPI dev server with hot-reload
make dbt-run         Run dbt models
make dbt-test        Run dbt schema tests
make gx-run          Run Great Expectations validation

make test            pytest
make test-cov        pytest + HTML coverage report
make clean           Remove __pycache__, generated data
```

---

## Resume Bullet

*"Built CMS HealthFlow — a healthcare claims analytics pipeline processing 15M+ Medicare provider records using PySpark for distributed transformation, dbt for data modelling, and Great Expectations for data quality validation. Exposed analytics through a FastAPI REST API enabling provider comparison, procedure cost analysis, and hospital quality rankings across all 50 states. Orchestrated by Apache Airflow with observability via Prometheus and Grafana. Full stack runs via Docker Compose."*
