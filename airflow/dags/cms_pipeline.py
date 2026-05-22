"""
CMS HealthFlow — Main Airflow DAG

Orchestrates the full weekly pipeline:
  generate_sample → normalize → quality → aggregate → rank → load → validate

Uses PythonOperator to call Spark job functions directly — no subprocess overhead.
Schedule: weekly (CMS updates data periodically).
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

default_args = {
    "owner": "cms-healthflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
    "email_on_retry": False,
}


# ── Task callables ────────────────────────────────────────────────────────

def task_generate_sample(**ctx):
    """Generate synthetic CMS data if no real download exists."""
    import os
    from pathlib import Path
    sys.path.insert(0, "/opt/airflow/project")

    year = int(os.getenv("CMS_PROVIDER_DATASET_YEAR", 2022))
    real = Path(f"/opt/airflow/project/data/raw/provider/cms_provider_{year}.csv")
    sample = Path(f"/opt/airflow/project/data/raw/provider/cms_provider_{year}_sample.csv")

    if real.exists():
        log.info("Real CMS data found at %s — skipping sample generation", real)
        return

    if sample.exists():
        log.info("Sample data already exists at %s", sample)
        return

    import subprocess
    subprocess.run(
        ["python", "scripts/generate_sample_data.py"],
        cwd="/opt/airflow/project",
        check=True,
    )


def task_normalize(**ctx):
    sys.path.insert(0, "/opt/airflow/project")
    from spark.jobs.normalize import main
    main()


def task_quality(**ctx):
    sys.path.insert(0, "/opt/airflow/project")
    from spark.jobs.quality import main
    main()


def task_aggregate(**ctx):
    sys.path.insert(0, "/opt/airflow/project")
    from spark.jobs.aggregate import main
    main()


def task_rank(**ctx):
    sys.path.insert(0, "/opt/airflow/project")
    from spark.jobs.rankings import main
    main()


def task_load(**ctx):
    sys.path.insert(0, "/opt/airflow/project")
    import subprocess
    subprocess.run(
        ["python", "scripts/load_gold_to_postgres.py"],
        cwd="/opt/airflow/project",
        check=True,
    )


def task_validate(**ctx):
    """Run Great Expectations validation on the raw CSV."""
    import os
    from pathlib import Path
    sys.path.insert(0, "/opt/airflow/project")

    year = int(os.getenv("CMS_PROVIDER_DATASET_YEAR", 2022))
    # Prefer real data; fall back to sample
    for suffix in ("", "_sample"):
        path = Path(f"/opt/airflow/project/data/raw/provider/cms_provider_{year}{suffix}.csv")
        if path.exists():
            from ingestion.validate import validate_with_pandas
            passed = validate_with_pandas(path)
            if not passed:
                raise ValueError(f"Data quality validation FAILED for {path}")
            log.info("Validation passed for %s", path)
            return

    log.warning("No data file found to validate")


def task_notify(**ctx):
    run_id = ctx.get("run_id", "unknown")
    log.info("✅ CMS HealthFlow pipeline complete — run_id=%s", run_id)
    log.info("Gold tables updated: provider_profiles, procedure_costs, cost_by_geography")


# ── DAG definition ────────────────────────────────────────────────────────

with DAG(
    dag_id="cms_healthflow_pipeline",
    description="Weekly CMS healthcare claims processing pipeline",
    schedule="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["cms", "healthcare", "batch"],
    doc_md="""
## CMS HealthFlow Pipeline

End-to-end batch pipeline on CMS Medicare provider utilization data.

**Flow:** generate/download → normalize → quality score → aggregate → rank → load → validate

**Gold tables produced:**
- `gold.provider_profiles` — one row per NPI with specialty/state rankings
- `gold.procedure_costs` — cost stats per HCPCS code per state
- `gold.cost_by_geography` — avg payments by ZIP code

**API:** http://cms-api:8000/docs
    """,
) as dag:

    generate = PythonOperator(
        task_id="generate_sample_data",
        python_callable=task_generate_sample,
        doc="Generate synthetic CMS rows if no real download exists",
    )

    normalize = PythonOperator(
        task_id="spark_normalize",
        python_callable=task_normalize,
        doc="PySpark: normalize column names, cast types → Silver Parquet",
    )

    quality = PythonOperator(
        task_id="spark_quality_score",
        python_callable=task_quality,
        doc="PySpark: add 0-100 quality score + outlier flags",
    )

    aggregate = PythonOperator(
        task_id="spark_aggregate",
        python_callable=task_aggregate,
        doc="PySpark: build Gold aggregations — provider profiles, procedure costs, geo rollups",
    )

    rank = PythonOperator(
        task_id="spark_rank",
        python_callable=task_rank,
        doc="PySpark: window function rankings within specialty and state",
    )

    load = PythonOperator(
        task_id="load_gold_to_postgres",
        python_callable=task_load,
        doc="Load Gold Parquet → PostgreSQL gold schema",
    )

    validate = PythonOperator(
        task_id="validate_data_quality",
        python_callable=task_validate,
        doc="Great Expectations / pandas validation on raw data",
    )

    notify = PythonOperator(
        task_id="notify_success",
        python_callable=task_notify,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        doc="Log pipeline completion",
    )

    generate >> normalize >> quality >> aggregate >> rank >> load >> validate >> notify
