"""
Write job execution metadata to public.pipeline_runs.

Used at the end of every Spark job to record rows processed,
rows failed, status, and timing — enables incremental logic
and pipeline health monitoring.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import RealDictCursor

from spark.config import CONFIG

log = logging.getLogger(__name__)


def _get_conn():
    return psycopg2.connect(
        host=CONFIG.postgres_host,
        port=int(CONFIG.postgres_port),
        dbname=CONFIG.postgres_db,
        user=CONFIG.postgres_user,
        password=CONFIG.postgres_password,
    )


def start_run(pipeline_name: str, dataset: str) -> int:
    """Insert a 'running' row and return its ID."""
    sql = """
        INSERT INTO public.pipeline_runs (pipeline_name, dataset, status, started_at)
        VALUES (%s, %s, 'running', %s)
        RETURNING id
    """
    try:
        with _get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (pipeline_name, dataset, datetime.now(timezone.utc)))
            run_id = cur.fetchone()[0]
            conn.commit()
        log.debug("Pipeline run started: id=%d  %s/%s", run_id, pipeline_name, dataset)
        return run_id
    except Exception as exc:
        log.warning("Could not write pipeline run start: %s", exc)
        return -1


def finish_run(
    run_id: int,
    rows_processed: int,
    rows_failed: int = 0,
    error_message: str | None = None,
) -> None:
    """Update the pipeline_runs row with final status and counts."""
    status = "success" if error_message is None else "failed"
    sql = """
        UPDATE public.pipeline_runs
        SET status = %s,
            rows_processed = %s,
            rows_failed = %s,
            completed_at = %s,
            error_message = %s
        WHERE id = %s
    """
    try:
        with _get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                sql,
                (status, rows_processed, rows_failed, datetime.now(timezone.utc), error_message, run_id),
            )
            conn.commit()
        log.info(
            "Pipeline run finished: id=%d  status=%s  rows=%d  failed=%d",
            run_id, status, rows_processed, rows_failed,
        )
    except Exception as exc:
        log.warning("Could not write pipeline run finish: %s", exc)


@contextmanager
def pipeline_run(pipeline_name: str, dataset: str):
    """
    Context manager that automatically records start/finish for a job.

    Usage:
        with pipeline_run("normalize", "provider/2022") as run_id:
            ...do work...
            yield row_count   # or just exit normally
    """
    run_id = start_run(pipeline_name, dataset)
    rows = 0
    error = None
    try:
        yield lambda n: setattr(_holder := type("H", (), {"n": n}), "rows", n) or n
        # Callers assign rows via: rows = yield_fn(count)
    except Exception as exc:
        error = str(exc)
        raise
    finally:
        finish_run(run_id, rows, error_message=error)
