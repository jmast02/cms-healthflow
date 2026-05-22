"""Spark job configuration loaded from environment variables."""

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()


@dataclass
class SparkConfig:
    app_name: str = os.getenv("SPARK_APP_NAME", "cms-healthflow")
    master: str = os.getenv("SPARK_MASTER", "local[*]")
    driver_memory: str = os.getenv("SPARK_DRIVER_MEMORY", "4g")
    executor_memory: str = os.getenv("SPARK_EXECUTOR_MEMORY", "2g")

    # Paths
    raw_data_dir: str = os.getenv("CMS_DATA_DIR", "data/raw")
    parquet_dir: str = "data/parquet"
    delta_dir: str = "data/delta"

    # MinIO / S3
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    bucket_raw: str = os.getenv("MINIO_BUCKET_RAW", "cms-raw")

    # PostgreSQL
    postgres_host: str = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port: str = os.getenv("POSTGRES_PORT", "5432")
    postgres_db: str = os.getenv("POSTGRES_DB", "cms_healthflow")
    postgres_user: str = os.getenv("POSTGRES_USER", "healthflow")
    postgres_password: str = os.getenv("POSTGRES_PASSWORD", "healthflow_secret")

    # Dataset
    dataset_year: int = int(os.getenv("CMS_PROVIDER_DATASET_YEAR", 2022))

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    @property
    def jdbc_properties(self) -> dict:
        return {
            "user": self.postgres_user,
            "password": self.postgres_password,
            "driver": "org.postgresql.Driver",
        }


CONFIG = SparkConfig()
