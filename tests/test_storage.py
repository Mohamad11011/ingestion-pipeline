from __future__ import annotations

import pytest

from config.settings import get_settings
from storage.mongo import MongoRepository
from storage.object_storage import S3ObjectStorage

pymongo_errors = pytest.importorskip("pymongo.errors")

_LANDING_BUCKET = "test-landing"
_TRANSFORMED_BUCKET = "test-transformed"


def _record(identifier: str, partition_date: str, file_hash: str = "hash-a") -> dict:
    return {
        "body": "Workplace Relations Commission",
        "identifier": identifier,
        "title": "A decision",
        "description": "Parties",
        "date": "2024-01-15",
        "document_url": f"https://example.test/{identifier}.html",
        "partition_date": partition_date,
        "file_path": f"Workplace Relations Commission/{identifier}.html",
        "file_hash": file_hash,
    }


@pytest.fixture
def repo() -> MongoRepository:
    settings = get_settings().model_copy(update={"mongo_database": "wr_storage_test"})
    repository = MongoRepository(settings=settings)
    try:
        repository._client.admin.command("ping")
    except pymongo_errors.PyMongoError:
        pytest.skip("MongoDB is not reachable")
    repository.landing.drop()
    repository.transformed.drop()
    repository.ensure_indexes()
    yield repository
    repository.landing.drop()
    repository.transformed.drop()
    repository.close()


@pytest.fixture
def storage() -> S3ObjectStorage:
    settings = get_settings().model_copy(
        update={
            "s3_landing_bucket": _LANDING_BUCKET,
            "s3_transformed_bucket": _TRANSFORMED_BUCKET,
        }
    )
    client = S3ObjectStorage(settings=settings)
    try:
        client.ensure_buckets()
    except Exception:
        pytest.skip("MinIO is not reachable")
    return client


def test_upsert_is_idempotent_on_body_and_identifier(repo: MongoRepository) -> None:
    assert repo.upsert_landing(_record("ADJ-1", "2024-01-01")) is True
    assert repo.upsert_landing(_record("ADJ-1", "2024-01-01")) is False

    assert repo.landing.count_documents({}) == 1


def test_upsert_updates_changed_file_hash(repo: MongoRepository) -> None:
    repo.upsert_landing(_record("ADJ-1", "2024-01-01", file_hash="hash-a"))
    repo.upsert_landing(_record("ADJ-1", "2024-01-01", file_hash="hash-b"))

    stored = repo.find_landing("Workplace Relations Commission", "ADJ-1")
    assert stored is not None
    assert stored["file_hash"] == "hash-b"


def test_unique_index_rejects_duplicate_identity(repo: MongoRepository) -> None:
    repo.upsert_landing(_record("ADJ-1", "2024-01-01"))

    with pytest.raises(pymongo_errors.DuplicateKeyError):
        repo.landing.insert_one(_record("ADJ-1", "2024-01-01"))


def test_partition_range_is_half_open(repo: MongoRepository) -> None:
    for identifier, partition in [
        ("ADJ-1", "2023-12-01"),
        ("ADJ-2", "2024-01-01"),
        ("ADJ-3", "2024-02-01"),
        ("ADJ-4", "2024-03-01"),
    ]:
        repo.upsert_landing(_record(identifier, partition))

    found = [doc["identifier"] for doc in repo.iter_landing_by_partition_range("2024-01-01", "2024-03-01")]

    assert found == ["ADJ-2", "ADJ-3"]


def test_transformed_upsert_does_not_touch_landing(repo: MongoRepository) -> None:
    repo.upsert_landing(_record("ADJ-1", "2024-01-01", file_hash="landing-hash"))
    transformed = _record("ADJ-1", "2024-01-01", file_hash="transformed-hash")
    transformed["file_path"] = "ADJ-1.html"
    repo.upsert_transformed(transformed)

    landing = repo.find_landing("Workplace Relations Commission", "ADJ-1")
    assert landing is not None
    assert landing["file_hash"] == "landing-hash"
    assert repo.transformed.count_documents({}) == 1


def test_object_round_trip(storage: S3ObjectStorage) -> None:
    key = "Workplace Relations Commission/ADJ-1.html"
    storage.put(_LANDING_BUCKET, key, b"<html>decision</html>", "text/html")

    assert storage.get(_LANDING_BUCKET, key) == b"<html>decision</html>"
    assert storage.exists(_LANDING_BUCKET, key) is True


def test_exists_is_false_for_missing_key(storage: S3ObjectStorage) -> None:
    assert storage.exists(_LANDING_BUCKET, "nope/missing.html") is False
