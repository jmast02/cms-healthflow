# CMS HealthFlow

**End-to-end healthcare claims analytics pipeline** processing 15M+ Medicare provider records from CMS public data using PySpark, dbt, Great Expectations, FastAPI, and an interactive Streamlit dashboard.

---

## Dashboard

The Streamlit dashboard at **http://localhost:8501** provides 5 interactive views:

| Page | What it shows |
|---|---|
| 🏠 **Overview** | Live KPIs, national choropleth map, top specialties bar chart |
| 🔍 **Provider Explorer** | Search/filter 50k+ providers by state, specialty, ZIP, payment range — NPI drill-down |
| 💊 **Procedure Costs** | HCPCS code lookup, state-by-state bar chart, cost choropleth map |
| 📊 **Specialty Analytics** | Payment vs volume scatter, top specialties, national Medicare spending |
| 🗺️ **Geographic Analysis** | State-level heatmap + ZIP-code histogram drill-down |

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
│  hospitals.py                              │
│                                             │
│  Bronze → Silver → Gold (Parquet)          │
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
                └──────────────┬───────────────┘
                               │
                    ┌──────────┴──────────┐
                    ▼                     ▼
          ┌──────────────────┐  ┌──────────────────────┐
          │    Streamlit     │  │   Apache Airflow      │
          │    Dashboard     │  │   cms_healthflow_     │
          │    5 pages       │  │   pipeline DAG        │
          │  localhost:8501  │  │   8 tasks · @weekly   │
          └──────────────────┘  └──────────┬───────────┘
                                           │
                                ┌──────────┴──────────┐
                                ▼                     ▼
                      ┌──────────────┐     ┌──────────────────┐
                      │  Prometheus  │     │     Grafana       │
                      │   metrics    │     │    Dashboard      │
                      └──────────────┘     └──────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data source | CMS Open Data Portal — free, no auth |
| Big data processing | Apache Spark 3.5 (PySpark) |
| File format | Parquet (snappy compressed) |
| Object storage | MinIO (local S3-compatible) |
| Data warehouse | PostgreSQL 15 (Bronze / Silver / Gold) |
| Transformations | PySpark DataFrame API + dbt |
| Data quality | Great Expectations 0.18 |
| API | FastAPI + SQLAlchemy 2.0 + Pydantic |
| Dashboard | Streamlit + Plotly |
| Orchestration | Apache Airflow 2.9 |
| Observability | Prometheus + Grafana |
| Infrastructure | Docker Compose — zero local setup |

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

### 1. Clone and configure

```bash
git clone https://github.com/jmast02/cms-healthflow
cd cms-healthflow
cp .env.example .env
```

### 2. Start the full stack

```bash
make up
```

| Service | URL | Credentials |
|---|---|---|
| **Streamlit Dashboard** | http://localhost:8501 | — |
| FastAPI docs | http://localhost:8000/docs | — |
| Airflow | http://localhost:8090 | admin / admin |
| MinIO console | http://localhost:9001 | minioadmin / minioadmin |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | — |

### 3. Run the pipeline

```bash
make pipeline
```

This runs in Docker — no local Python required. It:
1. Generates 50k synthetic CMS rows (or use `make download` for the real 2 GB dataset)
2. Normalizes raw CSV → Silver Parquet (PySpark)
3. Quality scores each row (0–100)
4. Aggregates to Gold (provider profiles, procedure costs, geographic rollups)
5. Computes window function rankings (specialty + state)
6. Loads Gold Parquet → PostgreSQL

### 4. Open the dashboard

```bash
open http://localhost:8501
```

### 5. Hit the API directly

```bash
# Search Cardiology providers in Florida
curl "http://localhost:8000/api/v1/providers?state=FL&specialty=cardiology&limit=5"

# Compare what Medicare pays for a procedure across every state
curl "http://localhost:8000/api/v1/procedures/99213/costs"

# Specialty breakdown — which specialties bill the most?
curl "http://localhost:8000/api/v1/analytics/specialties"

# Pipeline health + live provider count
curl "http://localhost:8000/api/v1/health"
```

---

## API Reference

Full interactive docs at **http://localhost:8000/docs**

```
GET /api/v1/providers                     Search + filter providers
GET /api/v1/providers/{npi}               Full provider analytics profile
GET /api/v1/providers/{npi}/procedures    Procedure cost breakdown for provider's state
GET /api/v1/providers/state/{state}/top   Top-ranked providers in a state

GET /api/v1/procedures/{hcpcs_code}/costs Compare Medicare costs across all states
GET /api/v1/procedures                    Search procedures by description

GET /api/v1/hospitals/rankings            Hospital Compare quality rankings
GET /api/v1/hospitals/{facility_id}       Individual hospital profile

GET /api/v1/analytics/specialties         Payment stats by medical specialty
GET /api/v1/analytics/state-summary       Provider + payment summary by state
GET /api/v1/analytics/cost-by-geography   Avg Medicare costs by state + ZIP

GET /api/v1/health                        Pipeline health + live provider count
GET /metrics                              Prometheus scrape endpoint
```

---

## Project Structure

```
cms-healthflow/
├── streamlit/
│   ├── app.py                        # Overview: KPIs, choropleth, specialty chart
│   ├── api_client.py                 # Centralized API calls with caching
│   └── pages/
│       ├── 1_provider_search.py      # Provider Explorer
│       ├── 2_procedure_costs.py      # Procedure Cost Analyzer
│       ├── 3_specialty_analytics.py  # Specialty Analytics
│       ├── 4_geographic_analysis.py  # Geographic Heatmap
│       └── 5_hospital_rankings.py    # Hospital Rankings
│
├── ingestion/
│   ├── download.py          # Stream-download CMS CSVs with progress bar
│   ├── ingest.py            # Upload raw files to MinIO
│   └── validate.py          # Great Expectations validation suite
│
├── spark/
│   ├── jobs/
│   │   ├── normalize.py     # Bronze: schema normalisation + type casting
│   │   ├── quality.py       # Silver: 0-100 quality score + outlier flags
│   │   ├── aggregate.py     # Gold: provider profiles, procedure costs, geo rollups
│   │   ├── rankings.py      # Gold: window function specialty/state rankings
│   │   └── hospitals.py     # Gold: Hospital Compare quality processing
│   └── utils/
│       ├── schema.py        # CMS column mapping (handles year-to-year renames)
│       ├── session.py       # SparkSession factory + JDBC JAR auto-download
│       └── pipeline_log.py  # Writes job metadata to public.pipeline_runs
│
├── dbt/
│   ├── models/staging/      # stg_providers — clean bronze → silver view
│   └── models/marts/        # provider_profiles, procedure_costs, geography, hospitals
│
├── api/
│   ├── main.py              # FastAPI app — Prometheus instrumentation, CORS
│   ├── db.py                # SQLAlchemy engine + session dependency
│   ├── routers/             # providers, procedures, hospitals, analytics
│   ├── models/              # SQLAlchemy ORM → gold.* tables
│   └── schemas/             # Pydantic request/response validation
│
├── airflow/dags/
│   └── cms_pipeline.py      # 8-task PythonOperator DAG (weekly schedule)
│
├── gx/                      # Great Expectations context + expectation suite
├── observability/
│   ├── prometheus/prometheus.yml
│   └── grafana/             # Pre-provisioned 9-panel dashboard
│
├── scripts/
│   ├── generate_sample_data.py    # 50k synthetic CMS rows for local dev
│   ├── load_gold_to_postgres.py   # Spark: write Gold Parquet → PostgreSQL
│   └── reset_db.py                # Drop + recreate all schemas (dry-run safe)
│
├── sql/
│   ├── 01_init_schemas.sql  # bronze/silver/gold schemas + pipeline_runs table
│   └── 02_create_tables.sql # DDL with indexes for all gold.* tables
│
├── tests/
│   ├── conftest.py          # Session-scoped Spark session + shared fixtures
│   ├── test_spark_jobs.py
│   ├── test_api_endpoints.py
│   └── test_data_quality.py
│
├── Dockerfile               # API container — python:3.11-slim
├── Dockerfile.spark         # Spark jobs — python:3.11-slim + Java + JDBC JAR
├── Dockerfile.streamlit     # Dashboard — python:3.11-slim
├── Dockerfile.airflow       # Airflow — apache/airflow:2.9.0 + Java + PySpark
├── docker-compose.yml       # 8 services + spark job runners (--profile jobs)
├── Makefile                 # make up / pipeline / dashboard / test
└── pyproject.toml           # pytest config + project metadata
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
- **Bronze** — raw CMS data, column names normalized, no business logic
- **Silver** — cleaned, validated, quality-scored (0–100), outlier-flagged
- **Gold** — aggregated, ranked, analytics-ready; served by FastAPI and Streamlit

### Schema evolution handling
CMS renames columns between dataset years. `spark/utils/schema.py` maps all known variants to canonical internal names — adding support for a new year only requires extending `COLUMN_MAPPING`.

### Great Expectations data quality
- NPI completeness (never null, always 10-digit)
- State code validity (accepted US state set)
- Payment range validation (non-negative, max $1M)
- HCPCS code format regex
- Freshness checks (warn after 14 days, error after 30)

### Incremental-ready design
`public.pipeline_runs` tracks every job's start time, row count, and status — the foundation for processing only new/updated records on future runs.

---

## Running Tests

```bash
make test       # Full pytest suite inside Docker
make test-cov   # With HTML coverage report
```

14 tests across Spark jobs, API endpoints, and data quality validation — all passing.

---

## Makefile Reference

```
make up               Start full Docker stack
make down             Stop all containers

make generate-sample  Generate 50k synthetic CMS rows (no download needed)
make download         Download real CMS dataset (~2 GB)
make pipeline         Full pipeline: sample → normalize → quality → aggregate → rank → load

make spark-normalize  Bronze → Silver
make spark-quality    Quality scoring
make spark-aggregate  Silver → Gold
make spark-rank       Window function rankings
make spark-load       Gold Parquet → PostgreSQL

make dbt-run          Run dbt models
make dbt-test         Run dbt schema tests

make test             pytest (runs inside Docker)
make dashboard        Open Streamlit at http://localhost:8501
```
