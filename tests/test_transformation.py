from __future__ import annotations

import copy
from datetime import date
from pathlib import Path

import pytest

from config.settings import get_settings
from hashing.files import sha256_bytes
from models.documents import DOC, DOCX, HTML, PDF
from storage.mongo import MongoRepository
from transformation.transformer import TransformationPipeline

FIXTURE = Path(__file__).parent / "fixtures" / "sample_decision.html"
LANDING_BUCKET = "test-landing"
TRANSFORMED_BUCKET = "test-transformed"

PDF_BYTES = b"%PDF-1.4\n1 0 obj\n<< /Type /Catalog >>\nendobj\n%%EOF\n"
DOC_BYTES = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1" + b"legacy word payload"
DOCX_BYTES = b"PK\x03\x04" + b"zip payload"


class FakeStorage:
    """In-memory object storage that records every mutation, so landing writes are visible."""

    def __init__(self, objects: dict[tuple[str, str], bytes] | None = None) -> None:
        self.objects = dict(objects or {})
        self.mutations: list[tuple[str, str, str]] = []

    def ensure_buckets(self) -> None:
        pass

    def get(self, bucket: str, key: str) -> bytes:
        try:
            return self.objects[(bucket, key)]
        except KeyError:
            raise FileNotFoundError(f"{bucket}/{key}") from None

    def put(self, bucket: str, key: str, body: bytes, content_type: str) -> None:
        self.mutations.append(("put", bucket, key))
        self.objects[(bucket, key)] = body

    def exists(self, bucket: str, key: str) -> bool:
        return (bucket, key) in self.objects


class FakeRepository:
    def __init__(self, records: list[dict]) -> None:
        self.records = copy.deepcopy(records)
        self.transformed: list[dict] = []
        self.landing_writes: list[dict] = []

    def iter_landing_by_partition_range(self, start_date, end_date, batch_size: int = 500):
        for record in self.records:
            if str(start_date) <= record["partition_date"] < str(end_date):
                yield copy.deepcopy(record)

    def upsert_transformed(self, record: dict) -> bool:
        self.transformed.append(record)
        return True

    def upsert_landing(self, record: dict) -> bool:
        self.landing_writes.append(record)
        return True


def landing_record(identifier: str, document_type: str, partition_date: str = "2024-02-01") -> dict:
    extension = {HTML: ".html", PDF: ".pdf", DOC: ".doc", DOCX: ".docx"}[document_type]
    return {
        "body": "Workplace Relations Commission",
        "identifier": identifier,
        "title": "A decision",
        "description": "A Chef v A Health Service Provider",
        "date": "2024-02-01",
        "document_url": f"https://example.test/{identifier}{extension}",
        "source_url": f"https://example.test/{identifier}.html",
        "document_type": document_type,
        "partition_date": partition_date,
        "file_path": f"Workplace Relations Commission/{identifier}{extension}",
        "file_hash": "landing-hash",
    }


@pytest.fixture
def settings():
    return get_settings().model_copy(
        update={
            "s3_landing_bucket": LANDING_BUCKET,
            "s3_transformed_bucket": TRANSFORMED_BUCKET,
        }
    )


def build(records: list[dict], payloads: dict[str, bytes], settings):
    storage = FakeStorage(
        {(LANDING_BUCKET, r["file_path"]): payloads[r["identifier"]] for r in records}
    )
    repository = FakeRepository(records)
    return repository, storage, TransformationPipeline(repository, storage, settings)


@pytest.mark.parametrize(
    ("document_type", "payload", "extension"),
    [(PDF, PDF_BYTES, ".pdf"), (DOC, DOC_BYTES, ".doc"), (DOCX, DOCX_BYTES, ".docx")],
)
def test_binary_documents_pass_through_byte_identical(
    document_type: str, payload: bytes, extension: str, settings
) -> None:
    records = [landing_record("ADJ-00039955", document_type)]
    repository, storage, pipeline = build(records, {"ADJ-00039955": payload}, settings)

    stats = pipeline.run(date(2024, 2, 1), date(2024, 3, 1))

    key = f"ADJ-00039955{extension}"
    assert storage.objects[(TRANSFORMED_BUCKET, key)] == payload
    assert repository.transformed[0]["file_path"] == key
    assert repository.transformed[0]["file_hash"] == sha256_bytes(payload)
    assert (stats.records_found, stats.records_scraped, stats.failed) == (1, 1, 0)


def test_html_is_cleaned_and_hash_matches_transformed_bytes(settings) -> None:
    payload = FIXTURE.read_bytes()
    records = [landing_record("ADJ-00039955", HTML)]
    repository, storage, pipeline = build(records, {"ADJ-00039955": payload}, settings)

    pipeline.run(date(2024, 2, 1), date(2024, 3, 1))

    stored = storage.objects[(TRANSFORMED_BUCKET, "ADJ-00039955.html")]
    assert stored != payload
    assert b"Return to Search" not in stored
    assert b"ADJUDICATION OFFICER RECOMMENDATION" in stored
    assert repository.transformed[0]["file_hash"] == sha256_bytes(stored)
    assert repository.transformed[0]["file_hash"] != "landing-hash"


def test_every_output_is_named_identifier_dot_ext(settings) -> None:
    records = [
        landing_record("ADJ-00039955", PDF),
        landing_record("LCR22916", DOCX),
        landing_record("PWD246", HTML),
    ]
    payloads = {
        "ADJ-00039955": PDF_BYTES,
        "LCR22916": DOCX_BYTES,
        "PWD246": FIXTURE.read_bytes(),
    }
    repository, storage, pipeline = build(records, payloads, settings)

    pipeline.run(date(2024, 2, 1), date(2024, 3, 1))

    keys = sorted(key for bucket, key in storage.objects if bucket == TRANSFORMED_BUCKET)
    assert keys == ["ADJ-00039955.pdf", "LCR22916.docx", "PWD246.html"]


def test_landing_metadata_is_preserved_in_transformed_record(settings) -> None:
    records = [landing_record("ADJ-00039955", PDF)]
    repository, _, pipeline = build(records, {"ADJ-00039955": PDF_BYTES}, settings)

    pipeline.run(date(2024, 2, 1), date(2024, 3, 1))

    transformed = repository.transformed[0]
    for field in ("title", "description", "date", "body", "source_url", "document_url", "partition_date"):
        assert transformed[field] == records[0][field]


def test_partition_range_is_half_open(settings) -> None:
    records = [
        landing_record("BEFORE", PDF, partition_date="2023-12-01"),
        landing_record("INSIDE", PDF, partition_date="2024-01-01"),
        landing_record("LAST", PDF, partition_date="2024-02-01"),
        landing_record("AFTER", PDF, partition_date="2024-03-01"),
    ]
    payloads = {record["identifier"]: PDF_BYTES for record in records}
    repository, _, pipeline = build(records, payloads, settings)

    pipeline.run(date(2024, 1, 1), date(2024, 3, 1))

    assert [record["identifier"] for record in repository.transformed] == ["INSIDE", "LAST"]


def test_single_failure_does_not_abort_the_run(settings) -> None:
    records = [landing_record("MISSING", PDF), landing_record("ADJ-00039955", PDF)]
    payloads = {record["identifier"]: PDF_BYTES for record in records}
    repository, storage, pipeline = build(records, payloads, settings)
    del storage.objects[(LANDING_BUCKET, records[0]["file_path"])]

    stats = pipeline.run(date(2024, 2, 1), date(2024, 3, 1))

    assert (stats.records_found, stats.records_scraped, stats.failed) == (2, 1, 1)
    assert [record["identifier"] for record in repository.transformed] == ["ADJ-00039955"]


def test_landing_objects_are_untouched(settings) -> None:
    records = [landing_record("ADJ-00039955", HTML), landing_record("LCR22916", PDF)]
    payloads = {"ADJ-00039955": FIXTURE.read_bytes(), "LCR22916": PDF_BYTES}
    repository, storage, pipeline = build(records, payloads, settings)
    before = {key: value for key, value in storage.objects.items() if key[0] == LANDING_BUCKET}

    pipeline.run(date(2024, 2, 1), date(2024, 3, 1))

    after = {key: value for key, value in storage.objects.items() if key[0] == LANDING_BUCKET}
    assert after == before
    assert [bucket for _, bucket, _ in storage.mutations] == [TRANSFORMED_BUCKET] * 2
    assert repository.landing_writes == []


pymongo_errors = pytest.importorskip("pymongo.errors")


@pytest.fixture
def mongo_settings():
    return get_settings().model_copy(
        update={
            "mongo_database": "wr_transform_test",
            "s3_landing_bucket": LANDING_BUCKET,
            "s3_transformed_bucket": TRANSFORMED_BUCKET,
        }
    )


@pytest.fixture
def repo(mongo_settings) -> MongoRepository:
    repository = MongoRepository(settings=mongo_settings)
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


def test_landing_collection_is_identical_after_a_run(repo: MongoRepository, mongo_settings) -> None:
    records = [
        landing_record("ADJ-00039955", HTML, partition_date="2024-02-01"),
        landing_record("LCR22916", PDF, partition_date="2024-02-01"),
        landing_record("OUTSIDE", PDF, partition_date="2024-04-01"),
    ]
    for record in records:
        repo.upsert_landing(record)
    before = list(repo.landing.find({}).sort("identifier"))

    storage = FakeStorage(
        {
            (LANDING_BUCKET, records[0]["file_path"]): FIXTURE.read_bytes(),
            (LANDING_BUCKET, records[1]["file_path"]): PDF_BYTES,
            (LANDING_BUCKET, records[2]["file_path"]): PDF_BYTES,
        }
    )
    stats = TransformationPipeline(repo, storage, mongo_settings).run(
        date(2024, 2, 1), date(2024, 3, 1)
    )

    assert list(repo.landing.find({}).sort("identifier")) == before
    assert (stats.records_found, stats.records_scraped, stats.failed) == (2, 2, 0)
    assert repo.transformed.count_documents({}) == 2
    assert sorted(doc["file_path"] for doc in repo.transformed.find({})) == [
        "ADJ-00039955.html",
        "LCR22916.pdf",
    ]


def test_rerunning_the_same_range_does_not_duplicate_transformed_records(
    repo: MongoRepository, mongo_settings
) -> None:
    record = landing_record("ADJ-00039955", PDF, partition_date="2024-02-01")
    repo.upsert_landing(record)
    storage = FakeStorage({(LANDING_BUCKET, record["file_path"]): PDF_BYTES})
    pipeline = TransformationPipeline(repo, storage, mongo_settings)

    pipeline.run(date(2024, 2, 1), date(2024, 3, 1))
    pipeline.run(date(2024, 2, 1), date(2024, 3, 1))

    assert repo.transformed.count_documents({}) == 1
