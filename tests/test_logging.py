from __future__ import annotations

import io
import json
import logging

from observability.structured import JsonFormatter, RunStats, log_event, log_failure


def _capture(name: str) -> tuple[logging.Logger, io.StringIO]:
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter())
    logger = logging.getLogger(name)
    logger.handlers = [handler]
    logger.propagate = False
    logger.setLevel(logging.INFO)
    return logger, stream


def _lines(stream: io.StringIO) -> list[dict]:
    return [json.loads(line) for line in stream.getvalue().splitlines()]


def test_event_is_json_with_partition_and_body() -> None:
    logger, stream = _capture("test.event")

    log_event(logger, "partition_started", partition="2024-01-01/2024-02-01", body="Labour Court")

    record = _lines(stream)[0]
    assert record["event"] == "partition_started"
    assert record["partition"] == "2024-01-01/2024-02-01"
    assert record["body"] == "Labour Court"
    assert record["level"] == "INFO"


def test_failure_logs_url_code_and_reason() -> None:
    logger, stream = _capture("test.failure")

    log_failure(logger, url="https://example.test/a.pdf", error_code=429, reason="rate limited")

    record = _lines(stream)[0]
    assert record["event"] == "record_failed"
    assert record["level"] == "ERROR"
    assert record["url"] == "https://example.test/a.pdf"
    assert record["error_code"] == "429"
    assert record["reason"] == "rate limited"


def test_run_summary_totals_scopes() -> None:
    logger, stream = _capture("test.summary")
    stats = RunStats()

    first = stats.scope(body="WRC", partition="2024-01-01/2024-02-01")
    first.records_found = 10
    first.records_scraped = 8
    first.failed = 2
    stats.close_scope(logger, first)

    second = stats.scope(body="Labour Court", partition="2024-01-01/2024-02-01")
    second.records_found = 5
    second.records_scraped = 5
    stats.close_scope(logger, second)

    stats.log_run_summary(logger)

    records = _lines(stream)
    assert records[0]["event"] == "partition_summary"
    assert records[0]["records_found"] == 10
    summary = records[-1]
    assert summary["event"] == "run_summary"
    assert summary["records_found"] == 15
    assert summary["records_scraped"] == 13
    assert summary["failed"] == 2
    assert summary["records_found"] == summary["records_scraped"] + summary["failed"]


def test_empty_partition_is_not_a_failure() -> None:
    logger, stream = _capture("test.empty")
    stats = RunStats()

    stats.close_scope(logger, stats.scope(body="Equality Tribunal", partition="2024-05-01/2024-06-01"))

    record = _lines(stream)[0]
    assert record["records_found"] == 0
    assert record["failed"] == 0
