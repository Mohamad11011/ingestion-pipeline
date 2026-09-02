import pytest
from pydantic import ValidationError

from config.settings import Settings, get_settings


def test_settings_load_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MONGO_URI", "mongodb://localhost:27017")
    monkeypatch.setenv("MONGO_DATABASE", "wr")
    monkeypatch.setenv("MONGO_LANDING_COLLECTION", "landing.metadata")
    monkeypatch.setenv("MONGO_TRANSFORMED_COLLECTION", "transformed.metadata")
    monkeypatch.setenv("S3_ENDPOINT", "http://localhost:9000")
    monkeypatch.setenv("S3_ACCESS_KEY", "key")
    monkeypatch.setenv("S3_SECRET_KEY", "secret")
    monkeypatch.setenv("S3_LANDING_BUCKET", "landing")
    monkeypatch.setenv("S3_TRANSFORMED_BUCKET", "transformed")
    monkeypatch.setenv("PARTITION_SIZE", "monthly")
    monkeypatch.setenv("SCRAPE_CONCURRENCY", "4")
    monkeypatch.setenv("SCRAPE_DELAY", "1.5")
    monkeypatch.setenv("SCRAPE_RETRY_TIMES", "2")
    monkeypatch.setenv("SCRAPE_USER_AGENT", "test-agent")
    get_settings.cache_clear()

    settings = Settings(_env_file=None)

    assert settings.mongo_uri == "mongodb://localhost:27017"
    assert settings.s3_landing_bucket == "landing"
    assert settings.s3_transformed_bucket == "transformed"
    assert settings.partition_size == "monthly"
    assert settings.scrape_concurrency == 4
    assert settings.scrape_delay == 1.5


def test_settings_require_mongo_uri(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MONGO_URI", raising=False)
    get_settings.cache_clear()

    with pytest.raises(ValidationError):
        Settings(_env_file=None)
