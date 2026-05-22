-- CMS HealthFlow — Schema Initialization
-- Creates Bronze / Silver / Gold medallion schemas and pipeline metadata table.
-- Runs automatically on first postgres container start via docker-entrypoint-initdb.d.

CREATE SCHEMA IF NOT EXISTS bronze;   -- raw ingested data, minimal transformation
CREATE SCHEMA IF NOT EXISTS silver;   -- cleaned, normalized, validated
CREATE SCHEMA IF NOT EXISTS gold;     -- analytics-ready aggregations and rankings

-- Pipeline run metadata (used by Airflow and quality gates)
CREATE TABLE IF NOT EXISTS public.pipeline_runs (
    id              SERIAL PRIMARY KEY,
    pipeline_name   VARCHAR(100) NOT NULL,
    dataset         VARCHAR(100) NOT NULL,
    status          VARCHAR(20)  NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    rows_processed  BIGINT,
    rows_failed     BIGINT,
    started_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMP,
    error_message   TEXT
);
