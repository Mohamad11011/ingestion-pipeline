from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=(_ROOT / ".env", _ROOT / ".env.example"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    mongo_uri: str
    mongo_database: str
    mongo_landing_collection: str
    mongo_transformed_collection: str
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    s3_landing_bucket: str
    s3_transformed_bucket: str
    s3_region: str = "us-east-1"
    partition_size: str = "monthly"
    scrape_concurrency: int = 8
    scrape_delay: float = 1.0
    scrape_retry_times: int = 3
    scrape_user_agent: str = "workplace-relations-scraper/0.1"


@lru_cache
def get_settings() -> Settings:
    return Settings()
