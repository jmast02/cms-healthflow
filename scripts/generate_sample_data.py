"""
Generate a small synthetic CMS provider CSV for local development and testing.

Produces data/raw/provider/cms_provider_sample.csv with realistic column
names, values, and distributions — no 2 GB download required.

Run: python scripts/generate_sample_data.py [--rows 50000] [--year 2022]
"""

import argparse
import random
import string
from pathlib import Path

import pandas as pd

SPECIALTIES = [
    "Internal Medicine", "Family Practice", "Cardiology", "Orthopedic Surgery",
    "Psychiatry", "Neurology", "Gastroenterology", "Dermatology",
    "Oncology", "Emergency Medicine", "Radiology", "Anesthesiology",
    "Ophthalmology", "Urology", "Obstetrics/Gynecology", "Pediatrics",
    "Nephrology", "Pulmonology", "Rheumatology", "Endocrinology",
]

HCPCS_CODES = [
    ("99213", "Office visit, established patient, low complexity"),
    ("99214", "Office visit, established patient, moderate complexity"),
    ("99215", "Office visit, established patient, high complexity"),
    ("99203", "Office visit, new patient, low complexity"),
    ("93000", "Electrocardiogram, routine ECG with interpretation"),
    ("71046", "Chest X-ray, 2 views"),
    ("80053", "Comprehensive metabolic panel"),
    ("85025", "Complete blood count with differential"),
    ("36415", "Routine venipuncture for collection of specimen"),
    ("99232", "Subsequent hospital care, moderate complexity"),
    ("99283", "Emergency dept visit, moderate severity"),
    ("27447", "Total knee arthroplasty"),
    ("66984", "Cataract surgery with IOL implant"),
    ("45378", "Colonoscopy, diagnostic"),
    ("70553", "MRI brain with contrast"),
]

STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
]

CITIES_BY_STATE = {
    "FL": ["Miami", "Orlando", "Tampa", "Jacksonville"],
    "CA": ["Los Angeles", "San Francisco", "San Diego", "Sacramento"],
    "TX": ["Houston", "Dallas", "Austin", "San Antonio"],
    "NY": ["New York", "Buffalo", "Albany", "Rochester"],
}
DEFAULT_CITIES = ["Springfield", "Riverside", "Madison", "Lincoln", "Georgetown"]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Anderson", "Taylor", "Thomas",
    "Hernandez", "Moore", "Martin", "Jackson", "Thompson", "White", "Lopez",
]
FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael",
    "Linda", "William", "Barbara", "David", "Elizabeth", "Richard", "Susan",
    "Joseph", "Sarah", "Thomas", "Karen", "Charles", "Nancy",
]


def random_npi() -> str:
    return "".join(random.choices(string.digits, k=10))


def generate_rows(n: int, year: int) -> list[dict]:
    rng = random.Random(42)
    npis = [random_npi() for _ in range(n // 3)]  # providers with multiple procedure rows

    rows = []
    for _ in range(n):
        npi = rng.choice(npis)
        specialty = rng.choice(SPECIALTIES)
        state = rng.choice(STATES)
        city = rng.choice(CITIES_BY_STATE.get(state, DEFAULT_CITIES))
        hcpcs_code, hcpcs_desc = rng.choice(HCPCS_CODES)
        last_name = rng.choice(LAST_NAMES)
        first_name = rng.choice(FIRST_NAMES)
        gender = rng.choice(["M", "F"])

        base_payment = rng.uniform(15, 800)
        submitted = base_payment * rng.uniform(1.5, 5.0)
        total_srvcs = rng.randint(5, 2000)
        total_benes = int(total_srvcs * rng.uniform(0.6, 0.95))

        rows.append({
            "Rndrng_Prvdr_NPI":              npi,
            "Rndrng_Prvdr_Last_Org_Name":    last_name,
            "Rndrng_Prvdr_First_Name":       first_name,
            "Rndrng_Prvdr_Crdntls":          rng.choice(["MD", "DO", "NP", "PA", "RN", None]),
            "Rndrng_Prvdr_Gndr":             gender,
            "Rndrng_Prvdr_Ent_Cd":           "I",
            "Rndrng_Prvdr_St1":              f"{rng.randint(100, 9999)} Main St",
            "Rndrng_Prvdr_St2":              None,
            "Rndrng_Prvdr_City":             city,
            "Rndrng_Prvdr_State_Abrvtn":     state,
            "Rndrng_Prvdr_State_FIPS":       str(rng.randint(1, 56)).zfill(2),
            "Rndrng_Prvdr_Zip5":             str(rng.randint(10000, 99999)),
            "Rndrng_Prvdr_Cntry":            "US",
            "Rndrng_Prvdr_Type":             specialty,
            "Rndrng_Prvdr_Mdcr_Prtcptg_Ind": rng.choice(["Y", "Y", "Y", "N"]),
            "HCPCS_Cd":                      hcpcs_code,
            "HCPCS_Desc":                    hcpcs_desc,
            "HCPCS_Drug_Ind":                "N",
            "Tot_Benes":                     total_benes,
            "Tot_Srvcs":                     total_srvcs,
            "Tot_Sbmtd_Chrg":                round(submitted * total_srvcs, 2),
            "Tot_Mdcr_Alowd_Amt":            round(base_payment * total_srvcs * 0.95, 2),
            "Tot_Mdcr_Pymt_Amt":             round(base_payment * total_srvcs, 2),
            "Tot_Mdcr_Stdzd_Amt":            round(base_payment * total_srvcs * 0.90, 2),
            "Avg_Sbmtd_Chrg":                round(submitted, 2),
            "Avg_Mdcr_Alowd_Amt":            round(base_payment * 0.95, 2),
            "Avg_Mdcr_Pymt_Amt":             round(base_payment, 2),
            "Avg_Mdcr_Stdzd_Amt":            round(base_payment * 0.90, 2),
        })

    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic CMS provider CSV")
    parser.add_argument("--rows", type=int, default=50_000, help="Number of rows (default: 50,000)")
    parser.add_argument("--year", type=int, default=2022, help="Dataset year label (default: 2022)")
    args = parser.parse_args()

    out_path = Path(f"data/raw/provider/cms_provider_{args.year}_sample.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Generating {args.rows:,} synthetic CMS rows → {out_path}")
    rows = generate_rows(args.rows, args.year)
    df = pd.DataFrame(rows)
    df.to_csv(out_path, index=False)

    size_kb = out_path.stat().st_size // 1024
    print(f"Done. {out_path}  ({size_kb:,} KB, {len(df):,} rows, {df['Rndrng_Prvdr_NPI'].nunique():,} unique NPIs)")


if __name__ == "__main__":
    main()
