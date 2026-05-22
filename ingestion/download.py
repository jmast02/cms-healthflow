"""
Download CMS public datasets from data.cms.gov.

Run directly:  python -m ingestion.download
               python -m ingestion.download --year 2022 --dataset provider
"""

import argparse
import hashlib
import logging
import os
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

DATA_DIR = Path(os.getenv("CMS_DATA_DIR", "data/raw"))

# CMS dataset catalog — add new datasets here as the project grows.
# The CMS open data API returns CSV via the /data.csv endpoint.
# CMS periodically changes download URLs when datasets are refreshed.
# Find the current URL at: https://data.cms.gov/provider-summary-by-type-of-service
# Click the dataset → "Export" → copy the CSV link and update below.
DATASETS: dict[str, dict] = {
    "provider": {
        "description": "Medicare Physician & Other Practitioners by Provider and Service",
        "years": {
            # Direct bulk CSV — verify at data.cms.gov if this 404s
            2022: "https://data.cms.gov/sites/default/files/2024-04/f8e26e6d-8cf6-4b13-a4b2-a22e6e3c8ba9/MUP_PHY_R24P04_0_Provider_by_Service_2022.csv",
            2021: "https://data.cms.gov/sites/default/files/2023-07/a399e5c2-b2b7-4f5e-9a8c-8e8e8e8e8e8e/MUP_PHY_R23P04_0_Provider_by_Service_2021.csv",
        },
        "filename_template": "cms_provider_{year}.csv",
    },
    "ipps": {
        "description": "Inpatient Prospective Payment System (IPPS) Provider Summary",
        "years": {
            2022: "https://data.cms.gov/sites/default/files/2023-09/97b32a99-09db-4b3f-9cbf-aabb2b6ce65c/FY2022_FR_IPPS_Provider_Data.csv",
        },
        "filename_template": "cms_ipps_{year}.csv",
    },
    "hospital_compare": {
        "description": "Hospital General Information (Hospital Compare)",
        "years": {
            2024: "https://data.cms.gov/provider-data/sites/default/files/resources/092256becd267d9eeccf73bf7d16c46b_1709752899/Hospital_General_Information.csv",
        },
        "filename_template": "cms_hospital_compare_{year}.csv",
    },
}


def _md5(path: Path, chunk_size: int = 1 << 20) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def download_file(url: str, dest: Path, force: bool = False) -> Path:
    """Stream-download *url* to *dest*, skipping if already present."""
    if dest.exists() and not force:
        log.info("Already downloaded: %s (%s MB)", dest.name, dest.stat().st_size // 1_048_576)
        return dest

    dest.parent.mkdir(parents=True, exist_ok=True)
    log.info("Downloading %s → %s", url, dest)

    with requests.get(url, stream=True, timeout=120) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))
        with (
            open(dest, "wb") as f,
            tqdm(total=total, unit="B", unit_scale=True, desc=dest.name) as bar,
        ):
            for chunk in resp.iter_content(chunk_size=1 << 16):
                f.write(chunk)
                bar.update(len(chunk))

    size_mb = dest.stat().st_size / 1_048_576
    log.info("Saved %s (%.1f MB)  md5=%s", dest.name, size_mb, _md5(dest))
    return dest


def download_dataset(name: str, year: int, force: bool = False) -> Path:
    """Download a named CMS dataset for a given year."""
    if name not in DATASETS:
        raise ValueError(f"Unknown dataset '{name}'. Available: {list(DATASETS)}")

    dataset = DATASETS[name]
    if year not in dataset["years"]:
        available = sorted(dataset["years"])
        raise ValueError(f"Year {year} not available for '{name}'. Available: {available}")

    url = dataset["years"][year]
    filename = dataset["filename_template"].format(year=year)
    dest = DATA_DIR / name / filename

    return download_file(url, dest, force=force)


def main() -> None:
    parser = argparse.ArgumentParser(description="Download CMS datasets")
    parser.add_argument("--dataset", default="provider", choices=list(DATASETS),
                        help="Which dataset to download (default: provider)")
    parser.add_argument("--year", type=int,
                        default=int(os.getenv("CMS_PROVIDER_DATASET_YEAR", 2022)),
                        help="Dataset year (default: 2022)")
    parser.add_argument("--force", action="store_true",
                        help="Re-download even if file already exists")
    args = parser.parse_args()

    try:
        path = download_dataset(args.dataset, args.year, force=args.force)
        log.info("Download complete: %s", path)
    except (ValueError, requests.HTTPError) as exc:
        log.error("Download failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
