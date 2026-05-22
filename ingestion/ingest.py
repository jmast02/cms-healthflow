"""
Upload raw CMS CSV files from data/raw/ to MinIO (local S3).

Run directly:  python -m ingestion.ingest
               python -m ingestion.ingest --dataset provider --year 2022
"""

import argparse
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

DATA_DIR = Path(os.getenv("CMS_DATA_DIR", "data/raw"))
BUCKET_RAW = os.getenv("MINIO_BUCKET_RAW", "cms-raw")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")


def get_minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


def ensure_bucket(client: Minio, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        log.info("Created bucket: %s", bucket)


def upload_file(client: Minio, local_path: Path, object_name: str, bucket: str = BUCKET_RAW) -> None:
    """Upload a local file to MinIO, overwriting if already present."""
    size = local_path.stat().st_size
    log.info("Uploading %s → s3://%s/%s (%.1f MB)", local_path.name, bucket, object_name, size / 1_048_576)

    client.fput_object(
        bucket_name=bucket,
        object_name=object_name,
        file_path=str(local_path),
        content_type="text/csv",
    )
    log.info("Upload complete: s3://%s/%s", bucket, object_name)


def ingest_dataset(dataset: str, year: int) -> None:
    filename = f"cms_{dataset}_{year}.csv"
    local_path = DATA_DIR / dataset / filename

    if not local_path.exists():
        log.error("File not found: %s. Run 'make download' first.", local_path)
        sys.exit(1)

    client = get_minio_client()
    ensure_bucket(client, BUCKET_RAW)

    object_name = f"{dataset}/{year}/{filename}"
    upload_file(client, local_path, object_name)


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload raw CMS files to MinIO")
    parser.add_argument("--dataset", default="provider", help="Dataset name (default: provider)")
    parser.add_argument("--year", type=int,
                        default=int(os.getenv("CMS_PROVIDER_DATASET_YEAR", 2022)),
                        help="Dataset year (default: 2022)")
    args = parser.parse_args()

    try:
        ingest_dataset(args.dataset, args.year)
    except S3Error as exc:
        log.error("MinIO error: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
