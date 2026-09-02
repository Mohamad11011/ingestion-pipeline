from __future__ import annotations

import sys
from pathlib import Path

import pytest
from parsel import Selector
from scrapy.http import HtmlResponse, Request

_SCRAPY_PROJECT = Path(__file__).resolve().parents[1] / "scrapy_project"
if str(_SCRAPY_PROJECT) not in sys.path:
    sys.path.insert(0, str(_SCRAPY_PROJECT))

from scraping.bodies import parse_bodies  # noqa: E402
from scraping.listing import next_page_href, parse_cards, parse_total_results  # noqa: E402
from workplace_scraper.spiders.workplace import WorkplaceSpider  # noqa: E402

FIXTURE = Path(__file__).parent / "fixtures" / "listing_labour_court_2024_01.html"
LISTING_URL = (
    "https://www.workplacerelations.ie/en/search/"
    "?decisions=1&from=1/1/2024&to=31/1/2024&body=3&pageNumber=1"
)


@pytest.fixture(scope="module")
def listing_html() -> bytes:
    return FIXTURE.read_bytes()


@pytest.fixture(scope="module")
def selector(listing_html: bytes) -> Selector:
    return Selector(text=listing_html.decode("utf-8", "replace"))


def _listing_response(html: bytes) -> HtmlResponse:
    meta = {
        "body": "Labour Court",
        "partition": "2024-01-01/2024-02-01",
        "partition_date": "2024-01-01",
        "page": 1,
    }
    request = Request(LISTING_URL, meta=meta)
    return HtmlResponse(url=LISTING_URL, body=html, encoding="utf-8", request=request)


def _spider() -> WorkplaceSpider:
    return WorkplaceSpider(start_date="2024-01-01", end_date="2024-02-01")


def test_bodies_are_discovered_from_the_site_filter(selector: Selector) -> None:
    bodies = parse_bodies(selector)

    assert {body.value for body in bodies} == {"1", "2", "3", "15376"}
    assert dict((body.value, body.name) for body in bodies)["15376"] == (
        "Workplace Relations Commission"
    )


def test_listing_page_yields_ten_cards(selector: Selector) -> None:
    cards = parse_cards(selector)

    assert len(cards) == 10
    first = cards[0]
    assert first.identifier == "LCR22912"
    assert first.date == "2024-01-30"
    assert first.url == "/en/cases/2024/february/lcr22912.html"
    assert "SONOMA VALLEY" in first.description


def test_total_results_and_next_page_are_parsed(selector: Selector) -> None:
    assert parse_total_results(selector) == 45
    assert "pageNumber=2" in next_page_href(selector)


def test_parse_listing_yields_document_requests_and_next_page(listing_html: bytes) -> None:
    spider = _spider()

    results = list(spider.parse_listing(_listing_response(listing_html)))

    documents = [r for r in results if r.callback == spider.parse_document]
    pages = [r for r in results if r.callback == spider.parse_listing]
    assert len(documents) == 10
    assert len(pages) == 1
    assert "pageNumber=2" in pages[0].url
    assert pages[0].meta["page"] == 2

    record = documents[0].meta["record"]
    assert record["identifier"] == "LCR22912"
    assert record["body"] == "Labour Court"
    assert record["partition_date"] == "2024-01-01"
    assert record["document_url"].endswith("/en/cases/2024/february/lcr22912.html")
    assert documents[0].meta["handle_httpstatus_list"] == [304]


def test_scope_counts_every_listing_hit(listing_html: bytes) -> None:
    spider = _spider()

    list(spider.parse_listing(_listing_response(listing_html)))

    scope = spider._scope("Labour Court", "2024-01-01/2024-02-01")
    assert scope.records_found == 10
    assert scope.failed == 0


def test_card_without_identifier_is_skipped_and_counted_as_failed() -> None:
    html = b"""
    <div class="item-list"><ul>
      <li class="each-item"><h2 class="title"><a href="/en/cases/x.html"></a></h2></li>
    </ul></div>
    """
    spider = _spider()

    results = list(spider.parse_listing(_listing_response(html)))

    assert results == []
    scope = spider._scope("Labour Court", "2024-01-01/2024-02-01")
    assert (scope.records_found, scope.records_scraped, scope.failed) == (1, 0, 1)


def test_empty_partition_is_not_a_failure() -> None:
    spider = _spider()

    results = list(spider.parse_listing(_listing_response(b"<html><body></body></html>")))

    assert results == []
    scope = spider._scope("Labour Court", "2024-01-01/2024-02-01")
    assert (scope.records_found, scope.failed) == (0, 0)


def test_listing_url_uses_inclusive_site_dates() -> None:
    spider = _spider()

    request = spider._listing_request("3", "Labour Court", spider.partitions[0], page=1)

    assert "from=1/1/2024" in request.url
    assert "to=31/1/2024" in request.url
    assert "body=3" in request.url
    assert request.meta["partition_date"] == "2024-01-01"


def test_spider_requires_iso_dates() -> None:
    with pytest.raises(ValueError):
        WorkplaceSpider(start_date="01-01-2024", end_date="2024-02-01")
    with pytest.raises(ValueError):
        WorkplaceSpider(start_date=None, end_date="2024-02-01")
