from datetime import date

import pytest

from scraping.runner import ScrapeSummary, build_command, summary_from_lines

dagster = pytest.importorskip("dagster")

from dagster import AssetKey, materialize  # noqa: E402
from tests.orchestration_loader import load_definitions  # noqa: E402


def test_build_command_passes_half_open_range_and_partition_size():
    command = build_command(date(2024, 1, 1), date(2024, 2, 1), "weekly", body_ids="3")
    assert "start_date=2024-01-01" in command
    assert "end_date=2024-02-01" in command
    assert "partition_size=weekly" in command
    assert "body_ids=3" in command


def test_build_command_omits_optional_args():
    command = build_command(date(2024, 1, 1), date(2024, 2, 1))
    assert not [arg for arg in command if arg.startswith(("partition_size", "body_ids"))]


def test_summary_from_lines_uses_the_run_summary_event():
    lines = [
        "not json",
        '{"event": "partition_summary", "records_found": 5}',
        '{"event": "run_summary", "records_found": 45, "records_scraped": 44, "failed": 1}',
    ]
    assert summary_from_lines(iter(lines)) == ScrapeSummary(45, 44, 1)


def test_summary_from_lines_without_a_summary_is_zeroed():
    assert summary_from_lines(iter(["{}"])) == ScrapeSummary()


def test_partition_size_selects_the_matching_dagster_partitions():
    monthly = load_definitions(partition_size="monthly")
    weekly = load_definitions(partition_size="weekly")
    monthly_partitions = monthly.build_partitions(monthly._settings)
    weekly_partitions = weekly.build_partitions(weekly._settings)
    assert type(monthly_partitions).__name__ == "MonthlyPartitionsDefinition"
    assert type(weekly_partitions).__name__ == "WeeklyPartitionsDefinition"


def test_unsupported_partition_size_is_rejected():
    module = load_definitions(partition_size="monthly")
    settings = module._settings.model_copy(update={"partition_size": "hourly"})
    with pytest.raises(ValueError, match="unsupported partition size"):
        module.build_partitions(settings)


def test_transformation_depends_on_ingestion():
    module = load_definitions()
    deps = module.transformed_documents.asset_deps
    assert deps[AssetKey(["transformed_documents"])] == {AssetKey(["landing_documents"])}


def test_monthly_partition_key_maps_to_a_half_open_window(monkeypatch):
    module = load_definitions()
    windows: list[tuple] = []

    monkeypatch.setattr(module, "run_spider", lambda *a, **kw: ScrapeSummary(3, 3, 0))
    monkeypatch.setattr(
        module,
        "transform_range",
        lambda start, end, settings=None: _stats(windows, start, end),
    )

    result = materialize(
        [module.landing_documents, module.transformed_documents],
        partition_key="2024-01-01",
    )
    assert result.success
    assert windows == [(date(2024, 1, 1), date(2024, 2, 1))]


def test_a_failing_scrape_fails_the_partition(monkeypatch):
    module = load_definitions()
    monkeypatch.setattr(module, "run_spider", lambda *a, **kw: ScrapeSummary(0, 0, 0, exit_code=1))
    with pytest.raises(Exception, match="scrapy exited with code 1"):
        materialize([module.landing_documents], partition_key="2024-01-01")


def _stats(sink, start, end):
    from observability.structured import RunStats

    sink.append((start, end))
    return RunStats(records_found=3, records_scraped=3, failed=0)
