from __future__ import annotations

import logging
from datetime import date, datetime
from urllib.parse import urlencode

import scrapy
from scrapy import signals
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError, TCPTimedOutError
from twisted.internet.error import TimeoutError as TxTimeoutError

from config.settings import get_settings
from observability.structured import RunStats, ScopeStats, configure_logging, log_event, log_failure
from partitioning.dates import DatePartition, generate_partitions
from scraping.bodies import parse_bodies
from scraping.ingest import conditional_headers
from scraping.listing import next_page_href, parse_cards, parse_total_results
from workplace_scraper.items import WorkplaceItem

SEARCH_URL = "https://www.workplacerelations.ie/en/search/"
ADVANCED_SEARCH_URL = f"{SEARCH_URL}?advance=true"
# Chatty at DEBUG once the JSON handler takes over the root logger.
NOISY_LOGGERS = ("botocore", "boto3", "s3transfer", "urllib3", "pymongo")


class WorkplaceSpider(scrapy.Spider):
    """Bodies x date partitions -> paginated listings -> decision documents -> landing zone."""

    name = "workplace"
    allowed_domains = ["workplacerelations.ie"]

    def __init__(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        partition_size: str | None = None,
        body_ids: str | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        settings = get_settings()
        self.start_date = _parse_date("start_date", start_date)
        self.end_date = _parse_date("end_date", end_date)
        self.partition_size = partition_size or settings.partition_size
        self.body_filter = {value.strip() for value in body_ids.split(",")} if body_ids else None
        self.partitions = generate_partitions(self.start_date, self.end_date, self.partition_size)
        self.ingestor = None
        self.run_stats = RunStats()
        self._scopes: dict[tuple[str, str], ScopeStats] = {}

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_opened(self, spider) -> None:
        configure_logging()
        for name in NOISY_LOGGERS:
            logging.getLogger(name).setLevel(logging.WARNING)

    async def start(self):
        log_event(
            self.logger,
            "run_started",
            start_date=self.start_date.isoformat(),
            end_date=self.end_date.isoformat(),
            partition_size=self.partition_size,
            partitions=len(self.partitions),
        )
        yield scrapy.Request(
            ADVANCED_SEARCH_URL,
            callback=self.parse_bodies,
            errback=self.on_request_error,
            dont_filter=True,
        )

    def parse_bodies(self, response):
        bodies = parse_bodies(response.selector)
        if self.body_filter:
            bodies = [body for body in bodies if body.value in self.body_filter]
        if not bodies:
            log_failure(self.logger, response.url, "no_bodies", "body discovery returned no bodies")
            return
        log_event(self.logger, "bodies_discovered", bodies=[body.name for body in bodies])

        for body in bodies:
            for partition in self.partitions:
                yield self._listing_request(body.value, body.name, partition, page=1)

    def parse_listing(self, response):
        body = response.meta["body"]
        partition = response.meta["partition"]
        scope = self._scope(body, partition)

        if response.meta["page"] == 1:
            log_event(
                self.logger,
                "partition_started",
                body=body,
                partition=partition,
                total_results=parse_total_results(response.selector),
            )

        cards = parse_cards(response.selector)
        for card in cards:
            scope.records_found += 1
            document_url = response.urljoin(card.url) if card.url else ""
            if not card.identifier or not document_url:
                scope.failed += 1
                log_failure(
                    self.logger,
                    document_url or response.url,
                    "missing_metadata",
                    "listing card without identifier or link",
                    body=body,
                    partition=partition,
                )
                continue

            record = {
                "identifier": card.identifier,
                "title": card.title or card.identifier,
                "description": card.description,
                "date": card.date,
                "document_url": document_url,
                "source_url": document_url,
                "partition_date": response.meta["partition_date"],
                "body": body,
            }
            existing = self._existing(body, card.identifier, document_url)
            yield scrapy.Request(
                document_url,
                callback=self.parse_document,
                errback=self.on_document_error,
                headers=conditional_headers(existing),
                meta={
                    "record": record,
                    "existing": existing,
                    "scope_key": (body, partition),
                    "handle_httpstatus_list": [304],
                },
            )

        next_href = next_page_href(response.selector)
        if cards and next_href:
            yield response.follow(
                next_href,
                callback=self.parse_listing,
                errback=self.on_request_error,
                meta={**response.meta, "page": response.meta["page"] + 1},
            )

    def parse_document(self, response):
        record = response.meta["record"]
        if response.status == 304:
            self.mark_scraped(response.meta["scope_key"])
            self.crawler.stats.inc_value("landing/unchanged_validator")
            log_event(
                self.logger,
                "document_unchanged",
                url=response.url,
                identifier=record["identifier"],
                body=record["body"],
                reason="http_validator",
            )
            return

        record["http_etag"] = _header(response, "ETag")
        record["http_last_modified"] = _header(response, "Last-Modified")
        yield WorkplaceItem(
            record=record,
            payload=response.body,
            content_type=_header(response, "Content-Type"),
            existing=response.meta["existing"],
            scope_key=response.meta["scope_key"],
        )

    def on_document_error(self, failure):
        request = failure.request
        error_code, reason = _failure_details(failure)
        body, partition = request.meta["scope_key"]
        self._scope(body, partition).failed += 1
        log_failure(
            self.logger,
            request.url,
            error_code,
            reason,
            body=body,
            partition=partition,
            identifier=request.meta["record"]["identifier"],
        )

    def on_request_error(self, failure):
        error_code, reason = _failure_details(failure)
        log_failure(self.logger, failure.request.url, error_code, reason)

    def mark_scraped(self, scope_key: tuple[str, str]) -> None:
        self._scope(*scope_key).records_scraped += 1

    def mark_failed(self, scope_key: tuple[str, str]) -> None:
        self._scope(*scope_key).failed += 1

    def spider_closed(self, spider, reason: str) -> None:
        for scope in self._scopes.values():
            self.run_stats.close_scope(self.logger, scope)
        self.run_stats.log_run_summary(
            self.logger,
            start_date=self.start_date.isoformat(),
            end_date=self.end_date.isoformat(),
            partition_size=self.partition_size,
            reason=reason,
        )

    def _existing(self, body: str, identifier: str, url: str) -> dict | None:
        if self.ingestor is None:
            return None
        try:
            return self.ingestor.existing(body, identifier)
        except Exception as exc:
            log_failure(self.logger, url, type(exc).__name__, f"metadata lookup failed: {exc}")
            return None

    def _listing_request(
        self, body_id: str, body_name: str, partition: DatePartition, page: int
    ) -> scrapy.Request:
        query = urlencode(
            {
                "decisions": "1",
                "from": partition.site_from(),
                "to": partition.site_to(),
                "body": body_id,
                "pageNumber": str(page),
            },
            safe="/",
        )
        return scrapy.Request(
            f"{SEARCH_URL}?{query}",
            callback=self.parse_listing,
            errback=self.on_request_error,
            meta={
                "body": body_name,
                "partition": _partition_label(partition),
                "partition_date": partition.partition_date.isoformat(),
                "page": page,
            },
        )

    def _scope(self, body: str, partition: str) -> ScopeStats:
        key = (body, partition)
        if key not in self._scopes:
            self._scopes[key] = self.run_stats.scope(body, partition)
        return self._scopes[key]


def _partition_label(partition: DatePartition) -> str:
    return f"{partition.start.isoformat()}/{partition.end.isoformat()}"


def _parse_date(name: str, value: str | None) -> date:
    if not value:
        raise ValueError(f"{name} is required, format YYYY-MM-DD")
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError(f"{name} must be YYYY-MM-DD, got {value!r}") from None


def _header(response, name: str) -> str | None:
    value = response.headers.get(name)
    return value.decode("latin-1") if value else None


def _failure_details(failure) -> tuple[str, str]:
    if failure.check(HttpError):
        status = failure.value.response.status
        return str(status), f"http error {status}"
    if failure.check(DNSLookupError):
        return "dns_lookup_error", str(failure.value)
    if failure.check(TxTimeoutError, TCPTimedOutError):
        return "timeout", str(failure.value)
    return failure.type.__name__, str(failure.value)
