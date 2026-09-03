from __future__ import annotations

import contextlib
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from scrapy.exceptions import DropItem
from scrapy.http import HtmlResponse, Request

_SCRAPY_PROJECT = Path(__file__).resolve().parents[1] / "scrapy_project"
if str(_SCRAPY_PROJECT) not in sys.path:
    sys.path.insert(0, str(_SCRAPY_PROJECT))

from config.settings import get_settings  # noqa: E402
from hashing.files import sha256_bytes  # noqa: E402
from models.documents import landing_key  # noqa: E402
from scraping.ingest import LandingIngestor, conditional_headers  # noqa: E402
from storage.mongo import MongoRepository  # noqa: E402
from workplace_scraper.pipelines import LandingPipeline  # noqa: E402
from workplace_scraper.spiders.workplace import WorkplaceSpider  # noqa: E402

pymongo_errors = pytest.importorskip("pymongo.errors")

FIXTURE = Path(__file__).parent / "fixtures" / "listing_labour_court_2024_01.html"
LISTING_URL = (
    "https://www.workplacerelations.ie/en/search/"
    "?decisions=1&from=1/1/2024&to=31/1/2024&body=3&pageNumber=1"
)
BUCKET = "landing"
BODY = "Labour Court"
PARTITION = "2024-01-01/2024-02-01"


class FakeStorage:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.puts: list[str] = []
        self.fail = False

    def put(self, bucket: str, key: str, body: bytes, content_type: str) -> None:
        if self.fail:
            raise OSError("object storage unavailable")
        self.puts.append(key)
        self.objects[(bucket, key)] = body

    def get(self, bucket: str, key: str) -> bytes:
        return self.objects[(bucket, key)]

    def exists(self, bucket: str, key: str) -> bool:
        return (bucket, key) in self.objects


class FakeRepository:
    def __init__(self) -> None:
        self.documents: dict[tuple[str, str], dict] = {}

    def find_landing(self, body: str, identifier: str) -> dict | None:
        found = self.documents.get((body, identifier))
        return dict(found) if found else None

    def upsert_landing(self, record: dict) -> bool:
        key = (record["body"], record["identifier"])
        inserted = key not in self.documents
        self.documents[key] = {**self.documents.get(key, {}), **record}
        return inserted

    def upsert_transformed(self, record: dict) -> bool:
        raise AssertionError("ingestion must not write the transformed collection")


class FakeSource:
    """Serves decision pages; honours If-None-Match so run #2 can skip a download."""

    def __init__(self, etag: str | None = None) -> None:
        self.etag = etag
        self.bodies: dict[str, bytes] = {}
        self.downloads: list[str] = []
        self.not_modified: list[str] = []

    def payload_for(self, url: str) -> bytes:
        return self.bodies.setdefault(url, f"<html>decision {url}</html>".encode())

    def fetch(self, request: Request) -> HtmlResponse:
        if self.etag and request.headers.get("If-None-Match", b"").decode() == self.etag:
            self.not_modified.append(request.url)
            return HtmlResponse(url=request.url, body=b"", status=304, request=request)
        self.downloads.append(request.url)
        headers = {"Content-Type": "text/html; charset=utf-8"}
        if self.etag:
            headers["ETag"] = self.etag
        return HtmlResponse(
            url=request.url,
            body=self.payload_for(request.url),
            headers=headers,
            encoding="utf-8",
            request=request,
        )


class FakeStats:
    def __init__(self) -> None:
        self.values: dict[str, int] = {}

    def inc_value(self, key: str, count: int = 1) -> None:
        self.values[key] = self.values.get(key, 0) + count


def _listing_response(html: bytes) -> HtmlResponse:
    meta = {"body": BODY, "partition": PARTITION, "partition_date": "2024-01-01", "page": 1}
    request = Request(LISTING_URL, meta=meta)
    return HtmlResponse(url=LISTING_URL, body=html, encoding="utf-8", request=request)


def _run(ingestor: LandingIngestor, source: FakeSource, html: bytes) -> WorkplaceSpider:
    spider = WorkplaceSpider(start_date="2024-01-01", end_date="2024-02-01")
    spider.ingestor = ingestor
    spider.crawler = SimpleNamespace(stats=FakeStats())
    pipeline = LandingPipeline()

    for request in spider.parse_listing(_listing_response(html)):
        if request.callback != spider.parse_document:
            continue
        for item in spider.parse_document(source.fetch(request)):
            with contextlib.suppress(DropItem):
                pipeline.process_item(item, spider)
    return spider


@pytest.fixture(scope="module")
def listing_html() -> bytes:
    return FIXTURE.read_bytes()


@pytest.fixture
def ingestor() -> LandingIngestor:
    return LandingIngestor(FakeRepository(), FakeStorage(), BUCKET)


def _repository(ingestor: LandingIngestor) -> FakeRepository:
    return ingestor._repository


def _storage(ingestor: LandingIngestor) -> FakeStorage:
    return ingestor._storage


def test_hash_and_key_come_from_the_shared_modules(ingestor: LandingIngestor) -> None:
    record = {
        "identifier": "LCR22912",
        "title": "LCR22912",
        "description": "Parties",
        "date": "2024-01-30",
        "document_url": "https://example.test/lcr22912.html",
        "source_url": "https://example.test/lcr22912.html",
        "partition_date": "2024-01-01",
        "body": BODY,
    }
    payload = b"<html>decision</html>"

    result = ingestor.ingest(record, payload, "text/html")

    assert result.file_hash == sha256_bytes(payload)
    assert result.file_path == landing_key(BODY, "LCR22912", "html")
    assert _storage(ingestor).objects[(BUCKET, result.file_path)] == payload


def test_second_run_creates_no_duplicates_and_writes_no_objects(
    ingestor: LandingIngestor, listing_html: bytes
) -> None:
    source = FakeSource()

    first = _run(ingestor, source, listing_html)
    assert len(_repository(ingestor).documents) == 10
    assert len(_storage(ingestor).puts) == 10
    scope = first._scope(BODY, PARTITION)
    assert (scope.records_found, scope.records_scraped, scope.failed) == (10, 10, 0)

    second = _run(ingestor, source, listing_html)

    assert len(_repository(ingestor).documents) == 10
    assert len(_storage(ingestor).puts) == 10
    assert second.crawler.stats.values["landing/unchanged_hash"] == 10
    assert "landing/objects_written" not in second.crawler.stats.values
    assert second.crawler.stats.values["landing/records_updated"] == 10


def test_changed_source_bytes_update_landing_object_and_metadata(
    ingestor: LandingIngestor, listing_html: bytes
) -> None:
    source = FakeSource()
    _run(ingestor, source, listing_html)
    changed_url = source.downloads[0]
    source.bodies[changed_url] = b"<html>revised decision</html>"

    second = _run(ingestor, source, listing_html)

    stored = _repository(ingestor).documents[(BODY, "LCR22912")]
    assert stored["file_hash"] == sha256_bytes(b"<html>revised decision</html>")
    objects = _storage(ingestor).objects
    assert objects[(BUCKET, stored["file_path"])] == b"<html>revised decision</html>"
    assert len(_storage(ingestor).puts) == 11
    assert second.crawler.stats.values["landing/objects_written"] == 1
    assert len(_repository(ingestor).documents) == 10


def test_stored_validator_skips_the_download(
    ingestor: LandingIngestor, listing_html: bytes
) -> None:
    source = FakeSource(etag='W/"v1"')

    _run(ingestor, source, listing_html)
    assert len(source.downloads) == 10
    assert _repository(ingestor).documents[(BODY, "LCR22912")]["http_etag"] == 'W/"v1"'

    second = _run(ingestor, source, listing_html)

    assert len(source.downloads) == 10
    assert len(source.not_modified) == 10
    assert len(_storage(ingestor).puts) == 10
    scope = second._scope(BODY, PARTITION)
    assert (scope.records_found, scope.records_scraped, scope.failed) == (10, 10, 0)


def test_conditional_headers_are_empty_without_stored_validators() -> None:
    assert conditional_headers(None) == {}
    assert conditional_headers({"file_hash": "abc"}) == {}
    stored = {"http_etag": 'W/"v1"', "http_last_modified": "Mon, 01 Jan 2024"}
    assert conditional_headers(stored) == {
        "If-None-Match": 'W/"v1"',
        "If-Modified-Since": "Mon, 01 Jan 2024",
    }


def test_storage_failure_is_counted_and_does_not_stop_the_run(
    ingestor: LandingIngestor, listing_html: bytes
) -> None:
    _storage(ingestor).fail = True

    spider = _run(ingestor, FakeSource(), listing_html)

    scope = spider._scope(BODY, PARTITION)
    assert (scope.records_found, scope.records_scraped, scope.failed) == (10, 0, 10)
    assert scope.records_found == scope.records_scraped + scope.failed
    assert _repository(ingestor).documents == {}


@pytest.fixture
def mongo_ingestor() -> LandingIngestor:
    settings = get_settings().model_copy(update={"mongo_database": "wr_idempotency_test"})
    repository = MongoRepository(settings=settings)
    try:
        repository._client.admin.command("ping")
    except pymongo_errors.PyMongoError:
        pytest.skip("MongoDB is not reachable")
    repository.landing.drop()
    repository.ensure_indexes()
    yield LandingIngestor(repository, FakeStorage(), BUCKET)
    repository.landing.drop()
    repository.close()


def test_repeated_runs_against_mongo_keep_one_document(
    mongo_ingestor: LandingIngestor, listing_html: bytes
) -> None:
    source = FakeSource()

    _run(mongo_ingestor, source, listing_html)
    _run(mongo_ingestor, source, listing_html)

    repository = mongo_ingestor._repository
    assert repository.landing.count_documents({}) == 10
    assert len(_storage(mongo_ingestor).puts) == 10

    changed_url = source.downloads[0]
    source.bodies[changed_url] = b"<html>revised</html>"
    _run(mongo_ingestor, source, listing_html)

    stored = repository.find_landing(BODY, "LCR22912")
    assert repository.landing.count_documents({}) == 10
    assert stored["file_hash"] == sha256_bytes(b"<html>revised</html>")
    assert stored["partition_date"] == "2024-01-01"
    assert len(_storage(mongo_ingestor).puts) == 11


RENDER_TELEMETRY = pytest.mark.parametrize(
    ("first_payload", "second_payload"),
    [
        pytest.param(
            b"<html>x</html><!-- Elapsed time: 0 -->",
            b"<html>x</html><!-- Elapsed time: 0.140607 -->",
            id="elapsed_time_changes",
        ),
        pytest.param(
            b"<html>x</html><!-- cached or not being index.aspx page --><!-- Elapsed time: 0 -->",
            b"<html>x</html><!-- Elapsed time: 0 -->",
            id="cache_state_comment_disappears",
        ),
        pytest.param(
            b"<html>x</html><!-- Elapsed time: 0 -->",
            b"<html>x</html><!-- cached or not being index.aspx page --><!-- Elapsed time: 0.4 -->",
            id="cache_state_comment_appears",
        ),
    ],
)


@RENDER_TELEMETRY
def test_render_telemetry_does_not_look_like_a_change(
    ingestor: LandingIngestor,
    first_payload: bytes,
    second_payload: bytes,
) -> None:
    record = {
        "identifier": "LCR22912",
        "title": "LCR22912",
        "description": "Parties",
        "date": "2024-01-30",
        "document_url": "https://example.test/lcr22912.html",
        "source_url": "https://example.test/lcr22912.html",
        "partition_date": "2024-01-01",
        "body": BODY,
    }
    first = ingestor.ingest(record, first_payload, "text/html")
    existing = _repository(ingestor).find_landing(BODY, "LCR22912")

    second = ingestor.ingest(record, second_payload, "text/html", existing)

    assert second.file_hash == first.file_hash
    assert second.object_written is False
    assert len(_storage(ingestor).puts) == 1
