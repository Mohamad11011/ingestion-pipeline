from __future__ import annotations

import logging

from scrapy.exceptions import DropItem

from config.settings import get_settings
from observability.structured import log_event, log_failure
from scraping.ingest import LandingIngestor
from storage.mongo import MongoRepository
from storage.object_storage import S3ObjectStorage

logger = logging.getLogger("workplace.landing")


class LandingPipeline:
    """Delegates every write to MongoRepository / S3ObjectStorage / LandingIngestor."""

    def open_spider(self, spider) -> None:
        settings = get_settings()
        self._repository = MongoRepository(settings=settings)
        self._repository.ensure_indexes()
        self._storage = S3ObjectStorage(settings=settings)
        self._storage.ensure_buckets()
        spider.ingestor = LandingIngestor(
            self._repository, self._storage, settings.s3_landing_bucket
        )

    def close_spider(self, spider) -> None:
        self._repository.close()

    def process_item(self, item, spider):
        record = item["record"]
        scope_key = item["scope_key"]
        try:
            result = spider.ingestor.ingest(
                record=record,
                payload=item["payload"],
                content_type=item.get("content_type"),
                existing=item.get("existing"),
            )
        except Exception as exc:
            spider.mark_failed(scope_key)
            log_failure(
                logger,
                record["document_url"],
                type(exc).__name__,
                f"landing write failed: {exc}",
                body=record["body"],
                identifier=record["identifier"],
            )
            raise DropItem(record["identifier"]) from exc

        spider.mark_scraped(scope_key)
        stats = spider.crawler.stats
        stats.inc_value(
            "landing/objects_written" if result.object_written else "landing/unchanged_hash"
        )
        stats.inc_value(
            "landing/records_inserted" if result.inserted else "landing/records_updated"
        )
        log_event(
            logger,
            "record_stored",
            body=record["body"],
            identifier=record["identifier"],
            partition=scope_key[1],
            file_path=result.file_path,
            file_hash=result.file_hash,
            document_type=result.document_type,
            object_written=result.object_written,
            inserted=result.inserted,
        )
        return item
