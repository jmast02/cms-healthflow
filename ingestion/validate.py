"""
Great Expectations validation suite for raw CMS provider data.

Run directly:  python -m ingestion.validate
               python -m ingestion.validate --path data/raw/provider/cms_provider_2022.csv
"""

import argparse
import logging
import os
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

VALID_US_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "DC", "PR", "VI", "GU", "AS", "MP", "ZZ",  # territories + unknown
}


def validate_with_great_expectations(csv_path: Path) -> bool:
    """Run GX expectation suite on the raw CMS provider CSV."""
    try:
        import great_expectations as gx
    except ImportError:
        log.warning("great-expectations not installed; falling back to pandas validation.")
        return validate_with_pandas(csv_path)

    log.info("Running Great Expectations validation on %s", csv_path)
    context = gx.get_context()

    # Build a GX datasource pointing at the CSV
    datasource = context.sources.add_pandas("cms_raw")
    asset = datasource.add_csv_asset("provider_claims", filepath_or_buffer=str(csv_path))
    batch_request = asset.build_batch_request()

    # Expectation suite
    suite_name = "cms_provider_suite"
    try:
        suite = context.get_expectation_suite(suite_name)
    except Exception:
        suite = context.add_expectation_suite(suite_name)

    validator = context.get_validator(batch_request=batch_request, expectation_suite=suite)

    # NPI must never be null
    validator.expect_column_values_to_not_be_null("Rndrng_Prvdr_NPI")

    # Average Medicare payment must be non-negative
    validator.expect_column_values_to_be_between(
        "Avg_Mdcr_Pymt_Amt", min_value=0, max_value=1_000_000
    )

    # Total services must be positive
    validator.expect_column_values_to_be_between("Tot_Srvcs", min_value=1)

    # State codes should be valid
    validator.expect_column_values_to_be_in_set(
        "Rndrng_Prvdr_State_Abrvtn", value_set=VALID_US_STATES
    )

    # HCPCS codes follow a known pattern (letter+4digits or 5digits)
    validator.expect_column_values_to_match_regex(
        "HCPCS_Cd", regex=r"^[A-Z0-9]\d{4}$"
    )

    validator.save_expectation_suite(discard_failed_expectations=False)

    results = validator.validate()
    passed = results.success
    stats = results.statistics

    log.info(
        "Validation %s — evaluated %d expectations, %d passed, %d failed",
        "PASSED" if passed else "FAILED",
        stats["evaluated_expectations"],
        stats["successful_expectations"],
        stats["unsuccessful_expectations"],
    )

    for result in results.results:
        if not result.success:
            log.warning("FAILED: %s", result.expectation_config.expectation_type)

    return passed


def validate_with_pandas(csv_path: Path) -> bool:
    """Lightweight pandas-based validation — fallback when GX is unavailable."""
    log.info("Running pandas validation on %s", csv_path)

    df = pd.read_csv(csv_path, dtype=str, nrows=10_000)  # sample first 10k rows for speed
    issues: list[str] = []

    npi_col = next((c for c in df.columns if "NPI" in c.upper()), None)
    if npi_col and df[npi_col].isna().any():
        null_count = df[npi_col].isna().sum()
        issues.append(f"{null_count} null NPIs in first 10k rows")

    state_col = next((c for c in df.columns if "STATE" in c.upper() and "ABRVTN" in c.upper()), None)
    if state_col:
        invalid_states = df[state_col].dropna()[~df[state_col].dropna().isin(VALID_US_STATES)]
        if not invalid_states.empty:
            issues.append(f"{len(invalid_states)} invalid state codes: {invalid_states.unique()[:5].tolist()}")

    if issues:
        for issue in issues:
            log.warning("Validation issue: %s", issue)
        return False

    log.info("Pandas validation passed (sampled 10k rows)")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate raw CMS CSV with Great Expectations")
    parser.add_argument(
        "--path",
        default=f"data/raw/provider/cms_provider_{os.getenv('CMS_PROVIDER_DATASET_YEAR', 2022)}.csv",
        help="Path to CMS CSV file",
    )
    args = parser.parse_args()

    csv_path = Path(args.path)
    if not csv_path.exists():
        log.error("File not found: %s", csv_path)
        sys.exit(1)

    passed = validate_with_great_expectations(csv_path)
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
