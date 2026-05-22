"""CMS HealthFlow — FastAPI application entrypoint."""

import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response
from prometheus_fastapi_instrumentator import Instrumentator

from api.db import check_connection
from api.routers import analytics, hospitals, procedures, providers
from api.schemas.provider import HealthResponse

# ── Custom pipeline metrics ──────────────────────────────────────────────
pipeline_rows_processed = Gauge(
    "cms_pipeline_rows_processed_total",
    "Total rows processed by the last pipeline run",
    ["job_name"],
)
pipeline_run_status = Gauge(
    "cms_pipeline_last_run_success",
    "1 if last pipeline run succeeded, 0 if failed",
    ["job_name"],
)
api_provider_searches = Counter(
    "cms_api_provider_searches_total",
    "Total provider search requests",
)
api_procedure_lookups = Counter(
    "cms_api_procedure_lookups_total",
    "Total procedure cost lookup requests",
)

app = FastAPI(
    title="CMS HealthFlow API",
    description=(
        "Healthcare provider analytics built on CMS public data. "
        "Search providers, compare procedure costs, and explore Medicare payment patterns."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# Auto-instrument all routes: request count, latency histograms, status codes
Instrumentator().instrument(app).expose(app, endpoint="/metrics", include_in_schema=False)

app.include_router(providers.router, prefix="/api/v1")
app.include_router(procedures.router, prefix="/api/v1")
app.include_router(hospitals.router, prefix="/api/v1")
app.include_router(analytics.router, prefix="/api/v1")


@app.get("/api/v1/health", response_model=HealthResponse, tags=["meta"])
async def health_check():
    """Pipeline health and data freshness status."""
    db_ok = check_connection()
    total_providers = None

    if db_ok:
        try:
            from sqlalchemy import text
            from api.db import engine
            with engine.connect() as conn:
                result = conn.execute(
                    text("SELECT COUNT(*) FROM gold.provider_profiles")
                )
                total_providers = result.scalar()
        except Exception:
            pass  # gold schema not yet populated — that's fine

    return {
        "status": "ok" if db_ok else "degraded",
        "database": "connected" if db_ok else "unavailable",
        "dataset_year": int(os.getenv("CMS_PROVIDER_DATASET_YEAR", 2022)),
        "total_providers": total_providers,
    }


@app.get("/", include_in_schema=False)
async def root():
    return {"message": "CMS HealthFlow API — visit /docs for the full API reference"}
