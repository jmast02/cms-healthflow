"""
Data quality tests for the ingestion validation layer.

Tests the pandas fallback validation logic without requiring
Great Expectations or real CMS data.

Run: pytest tests/test_data_quality.py -v
"""

import io
import tempfile
from pathlib import Path

import pandas as pd
import pytest


def make_csv(rows: list[dict]) -> Path:
    df = pd.DataFrame(rows)
    tmp = tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w")
    df.to_csv(tmp.name, index=False)
    return Path(tmp.name)


VALID_ROW = {
    "Rndrng_Prvdr_NPI": "1234567890",
    "Rndrng_Prvdr_Last_Org_Name": "Smith",
    "Rndrng_Prvdr_State_Abrvtn": "FL",
    "HCPCS_Cd": "99213",
    "Tot_Srvcs": "150",
    "Avg_Mdcr_Pymt_Amt": "75.50",
}


class TestPandasValidation:
    def test_valid_data_passes(self):
        from ingestion.validate import validate_with_pandas

        path = make_csv([VALID_ROW] * 5)
        assert validate_with_pandas(path) is True

    def test_null_npi_fails(self):
        from ingestion.validate import validate_with_pandas

        rows = [VALID_ROW.copy() for _ in range(5)]
        rows[0]["Rndrng_Prvdr_NPI"] = None
        path = make_csv(rows)
        assert validate_with_pandas(path) is False

    def test_invalid_state_fails(self):
        from ingestion.validate import validate_with_pandas

        rows = [VALID_ROW.copy() for _ in range(5)]
        rows[0]["Rndrng_Prvdr_State_Abrvtn"] = "XX"  # not a valid state
        path = make_csv(rows)
        assert validate_with_pandas(path) is False

    def test_all_valid_states_pass(self):
        from ingestion.validate import VALID_US_STATES, validate_with_pandas

        rows = []
        for state in list(VALID_US_STATES)[:10]:
            row = VALID_ROW.copy()
            row["Rndrng_Prvdr_State_Abrvtn"] = state
            rows.append(row)

        path = make_csv(rows)
        assert validate_with_pandas(path) is True


class TestColumnMapping:
    def test_all_canonical_names_covered(self):
        from spark.utils.schema import COLUMN_MAPPING

        expected_canonical = {
            "provider_npi", "provider_name", "provider_state",
            "hcpcs_code", "avg_medicare_payment", "total_services",
        }
        assert expected_canonical.issubset(set(COLUMN_MAPPING.values()))

    def test_no_duplicate_canonical_targets(self):
        from spark.utils.schema import COLUMN_MAPPING

        canonical_values = list(COLUMN_MAPPING.values())
        assert len(canonical_values) == len(set(canonical_values)), (
            "Duplicate canonical column names in COLUMN_MAPPING"
        )
