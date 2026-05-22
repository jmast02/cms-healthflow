"""CMS column name normalization and type definitions.

CMS renames columns between dataset years (sometimes dramatically).
This module centralises all mappings so jobs don't need to know
which year's naming convention they received.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    ShortType,
    StringType,
    StructField,
    StructType,
)

# Maps every known CMS column variant to the canonical internal name.
# Add new entries here when a new dataset year ships with different names.
COLUMN_MAPPING: dict[str, str] = {
    # Provider identity
    "Rndrng_Prvdr_NPI":              "provider_npi",
    "NPI":                           "provider_npi",
    "Rndrng_Prvdr_Last_Org_Name":    "provider_name",
    "Rndrng_Prvdr_First_Name":       "provider_first_name",
    "Rndrng_Prvdr_Crdntls":          "provider_credentials",
    "Rndrng_Prvdr_Gndr":             "provider_gender",
    "Rndrng_Prvdr_Ent_Cd":           "provider_entity_type",
    # Address
    "Rndrng_Prvdr_St1":              "provider_street1",
    "Rndrng_Prvdr_St2":              "provider_street2",
    "Rndrng_Prvdr_City":             "provider_city",
    "Rndrng_Prvdr_State_FIPS":       "provider_state_fips",
    "Rndrng_Prvdr_State_Abrvtn":     "provider_state",
    "Rndrng_Prvdr_Zip5":             "provider_zip",
    "Rndrng_Prvdr_Cntry":            "provider_country",
    # Provider type
    "Rndrng_Prvdr_Type":             "provider_type",
    "Rndrng_Prvdr_Mdcr_Prtcptg_Ind": "medicare_participation",
    # HCPCS procedure
    "HCPCS_Cd":                      "hcpcs_code",
    "HCPCS_Desc":                    "hcpcs_description",
    "HCPCS_Drug_Ind":                "hcpcs_drug_indicator",
    # Volume metrics
    "Tot_Benes":                     "total_beneficiaries",
    "Tot_Srvcs":                     "total_services",
    # Financial metrics
    "Tot_Sbmtd_Chrg":                "total_submitted_charge",
    "Tot_Mdcr_Alowd_Amt":            "total_medicare_allowed",
    "Tot_Mdcr_Pymt_Amt":             "total_medicare_payment",
    "Tot_Mdcr_Stdzd_Amt":            "total_medicare_standard",
    "Avg_Sbmtd_Chrg":                "avg_submitted_charge",
    "Avg_Mdcr_Alowd_Amt":            "avg_medicare_allowed",
    "Avg_Mdcr_Pymt_Amt":             "avg_medicare_payment",
    "Avg_Mdcr_Stdzd_Amt":            "avg_medicare_standard",
}

# Canonical schema after normalization — used for casting after rename.
PROVIDER_SCHEMA = StructType([
    StructField("provider_npi",            StringType(),        nullable=False),
    StructField("provider_name",           StringType(),        nullable=True),
    StructField("provider_first_name",     StringType(),        nullable=True),
    StructField("provider_credentials",    StringType(),        nullable=True),
    StructField("provider_gender",         StringType(),        nullable=True),
    StructField("provider_entity_type",    StringType(),        nullable=True),
    StructField("provider_street1",        StringType(),        nullable=True),
    StructField("provider_street2",        StringType(),        nullable=True),
    StructField("provider_city",           StringType(),        nullable=True),
    StructField("provider_zip",            StringType(),        nullable=True),
    StructField("provider_state",          StringType(),        nullable=True),
    StructField("provider_country",        StringType(),        nullable=True),
    StructField("provider_type",           StringType(),        nullable=True),
    StructField("medicare_participation",  StringType(),        nullable=True),
    StructField("hcpcs_code",              StringType(),        nullable=True),
    StructField("hcpcs_description",       StringType(),        nullable=True),
    StructField("hcpcs_drug_indicator",    StringType(),        nullable=True),
    StructField("total_beneficiaries",     DecimalType(15, 2),  nullable=True),
    StructField("total_services",          DecimalType(15, 2),  nullable=True),
    StructField("total_submitted_charge",  DecimalType(15, 2),  nullable=True),
    StructField("total_medicare_allowed",  DecimalType(15, 2),  nullable=True),
    StructField("total_medicare_payment",  DecimalType(15, 2),  nullable=True),
    StructField("total_medicare_standard", DecimalType(15, 2),  nullable=True),
    StructField("avg_submitted_charge",    DecimalType(15, 2),  nullable=True),
    StructField("avg_medicare_allowed",    DecimalType(15, 2),  nullable=True),
    StructField("avg_medicare_payment",    DecimalType(15, 2),  nullable=True),
    StructField("avg_medicare_standard",   DecimalType(15, 2),  nullable=True),
    StructField("dataset_year",            ShortType(),         nullable=True),
])

NUMERIC_COLS = [
    "total_beneficiaries", "total_services",
    "total_submitted_charge", "total_medicare_allowed",
    "total_medicare_payment", "total_medicare_standard",
    "avg_submitted_charge", "avg_medicare_allowed",
    "avg_medicare_payment", "avg_medicare_standard",
]


def normalize_column_names(df: DataFrame) -> DataFrame:
    """Rename raw CMS columns to canonical internal names."""
    for raw_name, canonical in COLUMN_MAPPING.items():
        if raw_name in df.columns:
            df = df.withColumnRenamed(raw_name, canonical)
    return df


def cast_numeric_columns(df: DataFrame) -> DataFrame:
    """Cast all financial and volume columns to Decimal, coercing bad values to NULL."""
    for col_name in NUMERIC_COLS:
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                F.col(col_name).cast(DecimalType(15, 2)),
            )
    return df


def drop_unknown_columns(df: DataFrame) -> DataFrame:
    """Remove any columns not in the canonical schema."""
    known = {f.name for f in PROVIDER_SCHEMA.fields}
    to_drop = [c for c in df.columns if c not in known]
    return df.drop(*to_drop) if to_drop else df
