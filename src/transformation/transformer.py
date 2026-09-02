from __future__ import annotations

import argparse
import logging
from datetime import date, datetime, timezone

from config.settings import Settings, get_settings
from hashing.files import sha256_bytes
from models.documents import DOC, DOCX, HTML, PDF, detect_document_type, transformed_key
from observability.structured import (
    RunStats,
    ScopeStats,
    configure_logging,
    log_event,
    log_failure,
)
from storage.mongo import MongoRepository
from storage.object_storage import S3ObjectStorage
from transformation.html import clean_html

logger = logging.getLogger("transformation")

CONTENT_TYPES = {
    HTML: "text/html; charset=utf-8",
    PDF: "application/pdf",
    DOC: "application/msword",
    DOCX: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
}


class TransformationPipeline:
    """Reads landing (metadata + objects) and writes only the transformed bucket/collection."""

    def __init__(
        self,
        repository: MongoRepository,
        storage: S3ObjectStorage,
        settings: Settings | None = None,
    ) -> None:
        self._repository = repository
        self._storage = storage
        self._settings = settings or get_settings()

    def run(self, start_date: date, end_date: date) -> RunStats:
        stats = RunStats()
        scopes: dict[tuple[str, str], ScopeStats] = {}
        partition_range = f"{start_date.isoformat()}/{end_date.isoformat()}"
        log_event(logger, "transform_started", partition=partition_range)

        for record in self._repository.iter_landing_by_partition_range(start_date, end_date):
            scope = self._scope_for(stats, scopes, record)
            scope.records_found += 1
            try:
                self._transform_record(record)
            except Exception as exc:
                scope.failed += 1
                log_failure(
                    logger,
                    url=record.get("document_url") or record.get("file_path") or "",
                    error_code=type(exc).__name__,
                    reason=str(exc),
                    identifier=record.get("identifier"),
                    body=record.get("body"),
                )
            else:
                scope.records_scraped += 1

        for scope in scopes.values():
            stats.close_scope(logger, scope)
        stats.log_run_summary(logger, partition=partition_range)
        return stats

    def _transform_record(self, record: dict) -> None:
        identifier = record.get("identifier")
        source_key = record.get("file_path")
        if not identifier or not record.get("body") or not source_key:
            raise ValueError("record is missing body, identifier or file_path")

        payload = self._storage.get(self._settings.s3_landing_bucket, source_key)
        document_type = _document_type(record, payload)
        output = clean_html(payload) if document_type == HTML else payload
        key = transformed_key(identifier, document_type)
        file_hash = sha256_bytes(output)

        self._storage.put(
            self._settings.s3_transformed_bucket,
            key,
            output,
            CONTENT_TYPES[document_type],
        )
        self._repository.upsert_transformed(
            {
                **record,
                "document_type": document_type,
                "file_path": key,
                "file_hash": file_hash,
                "source_file_path": source_key,
                "transformed_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        log_event(
            logger,
            "document_transformed",
            identifier=identifier,
            body=record.get("body"),
            partition=record.get("partition_date"),
            document_type=document_type,
            file_path=key,
            file_hash=file_hash,
            unchanged_content=document_type != HTML,
        )

    @staticmethod
    def _scope_for(
        stats: RunStats,
        scopes: dict[tuple[str, str], ScopeStats],
        record: dict,
    ) -> ScopeStats:
        key = (str(record.get("body")), str(record.get("partition_date")))
        if key not in scopes:
            scopes[key] = stats.scope(*key)
        return scopes[key]


def _document_type(record: dict, payload: bytes) -> str:
    stored = record.get("document_type")
    if stored in CONTENT_TYPES:
        return stored
    return detect_document_type(None, payload)


def transform_range(
    start_date: date,
    end_date: date,
    settings: Settings | None = None,
) -> RunStats:
    """Callable entry point for the CLI and for the orchestrator."""
    settings = settings or get_settings()
    repository = MongoRepository(settings=settings)
    storage = S3ObjectStorage(settings=settings)
    storage.ensure_buckets()
    try:
        return TransformationPipeline(repository, storage, settings).run(start_date, end_date)
    finally:
        repository.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Transform landing documents into the transformed zone.")
    parser.add_argument("--start-date", required=True, type=date.fromisoformat)
    parser.add_argument("--end-date", required=True, type=date.fromisoformat, help="exclusive")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    configure_logging(args.log_level)
    if args.end_date <= args.start_date:
        parser.error("--end-date must be after --start-date")

    stats = transform_range(args.start_date, args.end_date)
    return 1 if stats.records_scraped == 0 and stats.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
