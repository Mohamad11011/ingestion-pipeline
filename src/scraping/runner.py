from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import date
from pathlib import Path

logger = logging.getLogger("scraping.runner")

SCRAPY_PROJECT_DIR = Path(__file__).resolve().parents[2] / "scrapy_project"
SRC_DIR = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class ScrapeSummary:
    """The `run_summary` JSON log line emitted by the spider when it closes."""

    records_found: int = 0
    records_scraped: int = 0
    failed: int = 0
    exit_code: int = 0

    @property
    def succeeded(self) -> bool:
        return self.exit_code == 0


def build_command(
    start_date: date,
    end_date: date,
    partition_size: str | None = None,
    body_ids: str | None = None,
    python: str | None = None,
) -> list[str]:
    command = [
        python or sys.executable,
        "-m",
        "scrapy",
        "crawl",
        "workplace",
        "-a",
        f"start_date={start_date.isoformat()}",
        "-a",
        f"end_date={end_date.isoformat()}",
    ]
    if partition_size:
        command += ["-a", f"partition_size={partition_size}"]
    if body_ids:
        command += ["-a", f"body_ids={body_ids}"]
    return command


def summary_from_lines(lines: Iterator[str]) -> ScrapeSummary:
    """Pick the run summary out of the spider's structured JSON log stream."""
    found = scraped = failed = 0
    for line in lines:
        try:
            payload = json.loads(line)
        except (TypeError, ValueError):
            continue
        if not isinstance(payload, dict):
            continue
        if payload.get("event") == "run_summary":
            found = int(payload.get("records_found", 0))
            scraped = int(payload.get("records_scraped", 0))
            failed = int(payload.get("failed", 0))
    return ScrapeSummary(records_found=found, records_scraped=scraped, failed=failed)


def run_spider(
    start_date: date,
    end_date: date,
    partition_size: str | None = None,
    body_ids: str | None = None,
    python: str | None = None,
    on_line: Callable[[str], None] | None = None,
) -> ScrapeSummary:
    """Run the Scrapy spider for one date range and return its run summary.

    The spider runs in a subprocess because Twisted's reactor cannot be restarted inside a
    single process: an orchestrator materializing more than one partition per worker would
    otherwise fail on the second run.
    """
    command = build_command(start_date, end_date, partition_size, body_ids, python)
    emit = on_line or (lambda line: print(line, flush=True))
    captured: list[str] = []

    process = subprocess.Popen(
        command,
        cwd=SCRAPY_PROJECT_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=_child_env(),
    )
    with process:
        for line in process.stdout:
            line = line.rstrip()
            captured.append(line)
            emit(line)
    exit_code = process.returncode

    summary = summary_from_lines(iter(captured))
    return ScrapeSummary(
        records_found=summary.records_found,
        records_scraped=summary.records_scraped,
        failed=summary.failed,
        exit_code=exit_code,
    )


def _child_env() -> dict[str, str]:
    env = dict(os.environ)
    existing = env.get("PYTHONPATH", "")
    parts = [str(SRC_DIR)] + ([existing] if existing else [])
    env["PYTHONPATH"] = os.pathsep.join(parts)
    return env
