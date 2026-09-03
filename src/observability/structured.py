from __future__ import annotations

import json
import logging
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, TextIO

_FIELDS_KEY = "_fields"
_RESERVED = set(logging.LogRecord("", 0, "", 0, "", None, None).__dict__) | {
    "message",
    "asctime",
    "taskName",
    _FIELDS_KEY,
}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
        }
        payload.update(getattr(record, _FIELDS_KEY, {}))
        payload.update(
            {key: value for key, value in record.__dict__.items() if key not in _RESERVED}
        )
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(level: str = "INFO", stream: TextIO | None = None) -> None:
    handler = logging.StreamHandler(stream or sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)


def log_event(logger: logging.Logger, event: str, level: int = logging.INFO, **fields: Any) -> None:
    logger.log(level, event, extra={_FIELDS_KEY: fields})


def log_failure(
    logger: logging.Logger,
    url: str,
    error_code: str | int,
    reason: str,
    **fields: Any,
) -> None:
    """A failed record is logged and skipped; it never aborts the run."""
    log_event(
        logger,
        "record_failed",
        level=logging.ERROR,
        url=url,
        error_code=str(error_code),
        reason=reason,
        **fields,
    )


@dataclass
class ScopeStats:
    """Counts for one body x partition scope: found == scraped + failed."""

    body: str
    partition: str
    records_found: int = 0
    records_scraped: int = 0
    failed: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "body": self.body,
            "partition": self.partition,
            "records_found": self.records_found,
            "records_scraped": self.records_scraped,
            "failed": self.failed,
        }


@dataclass
class RunStats:
    records_found: int = 0
    records_scraped: int = 0
    failed: int = 0
    scopes: list[ScopeStats] = field(default_factory=list)

    def scope(self, body: str, partition: str) -> ScopeStats:
        stats = ScopeStats(body=body, partition=partition)
        self.scopes.append(stats)
        return stats

    def close_scope(self, logger: logging.Logger, stats: ScopeStats) -> None:
        self.records_found += stats.records_found
        self.records_scraped += stats.records_scraped
        self.failed += stats.failed
        log_event(logger, "partition_summary", **stats.as_dict())

    def log_run_summary(self, logger: logging.Logger, **fields: Any) -> None:
        log_event(
            logger,
            "run_summary",
            records_found=self.records_found,
            records_scraped=self.records_scraped,
            failed=self.failed,
            **fields,
        )
