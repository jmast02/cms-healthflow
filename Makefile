.PHONY: help up down logs download ingest spark-normalize spark-aggregate spark-rank \
        api-up api-dev dbt-run dbt-test gx-run test test-cov clean

# ── Default ───────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "CMS HealthFlow — Make Targets"
	@echo "────────────────────────────────────────────────────────"
	@echo "  Infrastructure"
	@echo "    make up              Start full Docker stack"
	@echo "    make down            Stop and remove containers"
	@echo "    make logs            Tail all container logs"
	@echo ""
	@echo "  Dashboard"
	@echo "    make dashboard       Open Streamlit at http://localhost:8501"
	@echo ""
	@echo "  Data Ingestion"
	@echo "    make download        Download CMS datasets to data/raw/"
	@echo "    make ingest          Upload raw files to MinIO"
	@echo ""
	@echo "  Spark Jobs"
	@echo "    make spark-normalize Normalize raw CMS schema"
	@echo "    make spark-aggregate Aggregate to provider/procedure level"
	@echo "    make spark-rank      Compute specialty and geo rankings"
	@echo ""
	@echo "  API"
	@echo "    make api-dev         Run FastAPI dev server (hot-reload)"
	@echo ""
	@echo "  dbt"
	@echo "    make dbt-run         Run all dbt models"
	@echo "    make dbt-test        Run dbt schema tests"
	@echo ""
	@echo "  Data Quality"
	@echo "    make gx-run          Run Great Expectations validation suite"
	@echo ""
	@echo "  Tests"
	@echo "    make test            Run pytest suite"
	@echo "    make test-cov        Run pytest with coverage report"
	@echo ""
	@echo "  Utilities"
	@echo "    make clean           Remove generated data and __pycache__"
	@echo "────────────────────────────────────────────────────────"

# ── Infrastructure ────────────────────────────────────────────────────────
# Core stack (postgres, minio, airflow, api, streamlit, prometheus, grafana)
up:
	docker compose up -d

dashboard:
	open http://localhost:8501

# Optional: start Spark cluster (only needed when submitting to a remote cluster)
# Spark jobs default to local[*] mode and don't need this for local dev.
spark-up:
	docker compose --profile spark up -d spark-master spark-worker

down:
	docker compose down

logs:
	docker-compose logs -f

# All pipeline steps run inside Docker — no local Python env required.
# Usage: make generate-sample && make pipeline

# ── Data ─────────────────────────────────────────────────────────────────
# Generate 50k synthetic CMS rows (no download needed)
generate-sample:
	docker compose run --rm generate-sample

# Download real CMS data (~2 GB)
download:
	docker compose run --rm spark-normalize python -m ingestion.download

# Upload raw CSV to MinIO
ingest:
	docker compose run --rm spark-normalize python -m ingestion.ingest

# ── Spark Jobs ────────────────────────────────────────────────────────────
spark-normalize:
	docker compose run --rm spark-normalize

spark-quality:
	docker compose run --rm spark-quality

spark-aggregate:
	docker compose run --rm spark-aggregate

spark-rank:
	docker compose run --rm spark-rank

spark-load:
	docker compose run --rm spark-load

spark-hospitals:
	docker compose run --rm spark-hospitals

# ── Full pipeline (generate sample → normalize → quality → aggregate → rank → load) ──
pipeline: generate-sample spark-normalize spark-quality spark-aggregate spark-rank spark-load

# ── dbt ───────────────────────────────────────────────────────────────────
dbt-run:
	docker compose run --rm spark-normalize dbt run --project-dir dbt --profiles-dir dbt

dbt-test:
	docker compose run --rm spark-normalize dbt test --project-dir dbt --profiles-dir dbt

# ── Tests ─────────────────────────────────────────────────────────────────
test:
	docker compose run --rm spark-normalize pytest tests/ -v

test-cov:
	docker compose run --rm spark-normalize pytest tests/ -v --cov=. --cov-report=term

# ── Utilities ─────────────────────────────────────────────────────────────
clean:
	rm -rf data/parquet/* data/delta/*
