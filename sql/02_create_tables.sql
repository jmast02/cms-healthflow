-- CMS HealthFlow — Core Table Definitions

-- ── Bronze Layer ─────────────────────────────────────────────────────────
-- Raw CMS provider records exactly as ingested (column names normalized but
-- no business logic applied yet).
CREATE TABLE IF NOT EXISTS bronze.provider_claims (
    provider_npi            VARCHAR(20),
    provider_name           VARCHAR(255),
    provider_first_name     VARCHAR(100),
    provider_credentials    VARCHAR(50),
    provider_gender         CHAR(1),
    provider_entity_type    CHAR(1),
    provider_street1        VARCHAR(255),
    provider_street2        VARCHAR(255),
    provider_city           VARCHAR(100),
    provider_zip            VARCHAR(10),
    provider_state          CHAR(2),
    provider_country        CHAR(2),
    provider_type           VARCHAR(100),
    medicare_participation  CHAR(1),
    hcpcs_code              VARCHAR(10),
    hcpcs_description       TEXT,
    hcpcs_drug_indicator    CHAR(1),
    total_beneficiaries     NUMERIC,
    total_services          NUMERIC,
    total_submitted_charge  NUMERIC(15,2),
    total_medicare_allowed  NUMERIC(15,2),
    total_medicare_payment  NUMERIC(15,2),
    total_medicare_standard NUMERIC(15,2),
    avg_submitted_charge    NUMERIC(15,2),
    avg_medicare_allowed    NUMERIC(15,2),
    avg_medicare_payment    NUMERIC(15,2),
    avg_medicare_standard   NUMERIC(15,2),
    dataset_year            SMALLINT,
    ingested_at             TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bronze_provider_npi   ON bronze.provider_claims(provider_npi);
CREATE INDEX IF NOT EXISTS idx_bronze_hcpcs_code     ON bronze.provider_claims(hcpcs_code);
CREATE INDEX IF NOT EXISTS idx_bronze_provider_state ON bronze.provider_claims(provider_state);

-- ── Silver Layer ─────────────────────────────────────────────────────────
-- Cleaned and validated provider claims. NULLs handled, outliers flagged,
-- data types enforced, deduplication applied.
CREATE TABLE IF NOT EXISTS silver.provider_claims (
    LIKE bronze.provider_claims INCLUDING DEFAULTS,
    quality_score       NUMERIC(5,2),   -- 0-100, from Great Expectations run
    is_outlier_charge   BOOLEAN DEFAULT FALSE,
    is_outlier_payment  BOOLEAN DEFAULT FALSE,
    processed_at        TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_silver_provider_npi   ON silver.provider_claims(provider_npi);
CREATE INDEX IF NOT EXISTS idx_silver_hcpcs_code     ON silver.provider_claims(hcpcs_code);
CREATE INDEX IF NOT EXISTS idx_silver_provider_state ON silver.provider_claims(provider_state);
CREATE INDEX IF NOT EXISTS idx_silver_provider_type  ON silver.provider_claims(provider_type);

-- ── Gold Layer ────────────────────────────────────────────────────────────
-- Provider-level aggregated profiles.
CREATE TABLE IF NOT EXISTS gold.provider_profiles (
    provider_npi            VARCHAR(20) PRIMARY KEY,
    provider_name           VARCHAR(255),
    provider_type           VARCHAR(100),
    provider_state          CHAR(2),
    provider_city           VARCHAR(100),
    provider_zip            VARCHAR(10),
    provider_gender         CHAR(1),
    medicare_participation  CHAR(1),
    total_procedures        BIGINT,
    total_beneficiaries     BIGINT,
    total_services          BIGINT,
    total_medicare_payment  NUMERIC(18,2),
    avg_medicare_payment    NUMERIC(15,2),
    avg_submitted_charge    NUMERIC(15,2),
    unique_hcpcs_codes      INTEGER,
    specialty_rank          INTEGER,     -- rank within provider_type by avg payment
    state_rank              INTEGER,     -- rank within provider_state by avg payment
    dataset_year            SMALLINT,
    updated_at              TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gold_provider_type  ON gold.provider_profiles(provider_type);
CREATE INDEX IF NOT EXISTS idx_gold_provider_state ON gold.provider_profiles(provider_state);

-- Procedure-level cost analytics.
CREATE TABLE IF NOT EXISTS gold.procedure_costs (
    hcpcs_code              VARCHAR(10),
    hcpcs_description       TEXT,
    provider_state          CHAR(2),
    provider_count          INTEGER,
    total_services          BIGINT,
    avg_submitted_charge    NUMERIC(15,2),
    avg_medicare_payment    NUMERIC(15,2),
    median_medicare_payment NUMERIC(15,2),
    min_medicare_payment    NUMERIC(15,2),
    max_medicare_payment    NUMERIC(15,2),
    stddev_medicare_payment NUMERIC(15,2),
    dataset_year            SMALLINT,
    updated_at              TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (hcpcs_code, provider_state, dataset_year)
);

CREATE INDEX IF NOT EXISTS idx_gold_proc_hcpcs  ON gold.procedure_costs(hcpcs_code);
CREATE INDEX IF NOT EXISTS idx_gold_proc_state  ON gold.procedure_costs(provider_state);

-- Geographic cost heatmap data.
CREATE TABLE IF NOT EXISTS gold.cost_by_geography (
    provider_state          CHAR(2),
    provider_zip            VARCHAR(10),
    total_providers         INTEGER,
    total_services          BIGINT,
    avg_medicare_payment    NUMERIC(15,2),
    avg_submitted_charge    NUMERIC(15,2),
    dataset_year            SMALLINT,
    updated_at              TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (provider_state, provider_zip, dataset_year)
);

-- Hospital Compare quality metrics (from CMS Hospital Compare dataset).
CREATE TABLE IF NOT EXISTS gold.hospital_rankings (
    facility_id             VARCHAR(20) PRIMARY KEY,
    facility_name           VARCHAR(255),
    address                 VARCHAR(255),
    city                    VARCHAR(100),
    state                   CHAR(2),
    zip_code                VARCHAR(10),
    county_name             VARCHAR(100),
    phone_number            VARCHAR(20),
    hospital_type           VARCHAR(100),
    hospital_ownership      VARCHAR(100),
    emergency_services      BOOLEAN,
    overall_rating          SMALLINT,       -- 1-5 stars (null = not rated)
    readmission_national    VARCHAR(50),    -- above/below/same as national rate
    mortality_national      VARCHAR(50),
    safety_national         VARCHAR(50),
    patient_experience      VARCHAR(50),
    effectiveness_national  VARCHAR(50),
    timeliness_national     VARCHAR(50),
    efficient_imaging       VARCHAR(50),
    state_rank              INTEGER,
    national_rank           INTEGER,
    dataset_year            SMALLINT,
    updated_at              TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gold_hosp_state  ON gold.hospital_rankings(state);
CREATE INDEX IF NOT EXISTS idx_gold_hosp_rating ON gold.hospital_rankings(overall_rating);
