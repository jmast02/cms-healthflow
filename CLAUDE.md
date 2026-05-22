# CMS HealthFlow — Claude Code Context

## What this project is
End-to-end healthcare claims analytics pipeline built on CMS (Centers for Medicare & Medicaid Services) public data. Portfolio/resume project targeting healthtech and enterprise DE roles.

## Architecture
- **Ingestion**: download CMS CSVs → MinIO (local S3)
- **Processing**: PySpark jobs (local[*] mode in Docker) — normalize → quality → aggregate → rank
- **Storage**: PostgreSQL with bronze/silver/gold medallion schemas + Parquet files
- **API**: FastAPI serving gold layer tables
- **Orchestration**: Airflow DAG (weekly schedule)
- **Observability**: Prometheus + Grafana

## How to run everything (zero local Python required)
```bash
docker compose up -d                    # start core stack
make generate-sample                    # generate 50k synthetic rows
make spark-normalize                    # Bronze → Silver Parquet
make spark-quality                      # quality score each row
make spark-aggregate                    # Silver → Gold Parquet
make spark-rank                         # add window function rankings
make spark-load                         # load Gold Parquet → PostgreSQL
curl http://localhost:8000/api/v1/health
```

## Key files
- `docker-compose.yml` — all services; spark jobs use `--profile jobs`
- `Dockerfile` — FastAPI container (no Java, slim)
- `Dockerfile.spark` — PySpark container (includes Java + JDBC JAR)
- `spark/utils/session.py` — SparkSession factory; sets PYSPARK_SUBMIT_ARGS for JDBC JAR
- `spark/utils/schema.py` — CMS column mapping (handles year-to-year renames)
- `scripts/load_gold_to_postgres.py` — pushes Gold Parquet → PostgreSQL after Spark jobs
- `sql/02_create_tables.sql` — DDL for all gold.* tables

## Streamlit Dashboard
- `streamlit/app.py` — home page (overview KPIs, choropleth, specialty chart)
- `streamlit/pages/1_provider_search.py` — provider search with drill-down
- `streamlit/pages/2_procedure_costs.py` — HCPCS cost comparison across states
- `streamlit/pages/3_specialty_analytics.py` — specialty payment analytics
- `streamlit/pages/4_geographic_analysis.py` — state + ZIP heatmaps
- `streamlit/pages/5_hospital_rankings.py` — Hospital Compare quality ratings
- `streamlit/api_client.py` — all API calls centralized here with `@st.cache_data`
- `Dockerfile.streamlit` — slim Python image, no Java needed
- Live reload: `./streamlit` is volume-mounted, code changes reflect immediately

## Port map
| Service | URL |
|---|---|
| **Streamlit** | http://localhost:8501 |
| FastAPI | http://localhost:8000/docs |
| Airflow | http://localhost:8090 (admin/admin) |
| MinIO console | http://localhost:9001 (minioadmin/minioadmin) |
| Grafana | http://localhost:3000 (admin/admin) |
| Prometheus | http://localhost:9090 |
| PostgreSQL | localhost:5433 (mapped from 5432 — local PG already on 5432) |

## Known constraints
- PostgreSQL host port is 5433 (not 5432) because the developer has a local PG instance on 5432
- Spark runs in `local[*]` mode inside Docker — no separate Spark cluster needed
- `bitnami/spark` has no arm64 image; use `apache/spark:latest` for the optional cluster
- `PYSPARK_SUBMIT_ARGS` must be set before JVM starts — `builder.config()` is too late for classpath entries
- Airflow removed from `requirements.txt` because it requires SQLAlchemy < 2.0; FastAPI needs 2.0+

## Data
- Primary: `data/raw/provider/cms_provider_2022.csv` (~2GB real) or `cms_provider_2022_sample.csv` (50k synthetic rows)
- Parquet outputs: `data/parquet/silver/` and `data/parquet/gold/`
- Gold tables in PostgreSQL: `gold.provider_profiles`, `gold.procedure_costs`, `gold.cost_by_geography`, `gold.hospital_rankings`
