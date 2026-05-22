"""
CMS HealthFlow — Main Airflow DAG

Orchestrates the full weekly pipeline:
  download → ingest → normalize → quality → aggregate → rank → dbt → validate

Schedule: weekly (CMS updates data periodically)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "cms-healthflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

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

    Full end-to-end batch pipeline processing CMS Medicare provider utilization data.

    ### Flow
    1. **download** — pull latest CMS CSV from data.cms.gov
    2. **ingest** — upload raw file to MinIO (s3://cms-raw)
    3. **validate_raw** — Great Expectations on raw CSV (quality gate)
    4. **spark_normalize** — Bronze: normalize schema, clean nulls
    5. **spark_quality** — Silver: score each row for data quality
    6. **spark_aggregate** — Gold: provider profiles, procedure costs, geo rollups
    7. **spark_rank** — Gold: window function rankings within specialty/state
    8. **dbt_run** — Final Gold models and mart tables in PostgreSQL
    9. **dbt_test** — Schema tests and freshness checks
    10. **notify_success** — Log pipeline completion metrics
    """,
) as dag:

    # ── Step 1: Download ──────────────────────────────────────────────────
    download = BashOperator(
        task_id="download_cms_data",
        bash_command="cd /opt/airflow && python -m ingestion.download --dataset provider --year 2022",
        doc="Download CMS provider utilization CSV from data.cms.gov",
    )

    # ── Step 2: Ingest to MinIO ───────────────────────────────────────────
    ingest = BashOperator(
        task_id="ingest_to_minio",
        bash_command="cd /opt/airflow && python -m ingestion.ingest --dataset provider --year 2022",
        doc="Upload raw CSV to MinIO s3://cms-raw",
    )

    # ── Step 3: Validate raw data (quality gate) ──────────────────────────
    validate_raw = BashOperator(
        task_id="validate_raw_data",
        bash_command=(
            "cd /opt/airflow && "
            "python -m ingestion.validate "
            "--path data/raw/provider/cms_provider_2022.csv"
        ),
        doc="Great Expectations validation — fails pipeline if data quality is unacceptable",
    )

    # ── Step 4: Spark — Normalize ─────────────────────────────────────────
    spark_normalize = BashOperator(
        task_id="spark_normalize",
        bash_command="cd /opt/airflow && python -m spark.jobs.normalize",
        doc="PySpark: normalize column names, cast types, drop null NPIs → Silver Parquet",
    )

    # ── Step 5: Spark — Quality scoring ──────────────────────────────────
    spark_quality = BashOperator(
        task_id="spark_quality_score",
        bash_command="cd /opt/airflow && python -m spark.jobs.quality",
        doc="PySpark: add quality_score and outlier flags to Silver layer",
    )

    # ── Step 6: Spark — Aggregate ─────────────────────────────────────────
    spark_aggregate = BashOperator(
        task_id="spark_aggregate",
        bash_command="cd /opt/airflow && python -m spark.jobs.aggregate",
        doc="PySpark: build Gold aggregations (provider profiles, procedure costs, geo rollups)",
    )

    # ── Step 7: Spark — Rankings ──────────────────────────────────────────
    spark_rank = BashOperator(
        task_id="spark_rank",
        bash_command="cd /opt/airflow && python -m spark.jobs.rankings",
        doc="PySpark: window function rankings within specialty and state",
    )

    # ── Step 8: dbt run ───────────────────────────────────────────────────
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir .",
        doc="dbt: run staging and mart models on top of Gold Parquet outputs",
    )

    # ── Step 9: dbt test ──────────────────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir .",
        doc="dbt: run schema tests (not_null, unique, accepted_values, freshness)",
    )

    # ── Step 10: Log success ──────────────────────────────────────────────
    def _log_success(**context):
        execution_date = context["execution_date"]
        print(f"Pipeline completed successfully for execution date: {execution_date}")
        print("All tasks passed. Data is ready in gold schema.")

    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=_log_success,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── DAG dependency chain ──────────────────────────────────────────────
    (
        download
        >> ingest
        >> validate_raw
        >> spark_normalize
        >> spark_quality
        >> spark_aggregate
        >> spark_rank
        >> dbt_run
        >> dbt_test
        >> notify_success
    )
