import sys
from datetime import date
from pathlib import Path

_SRC = Path(__file__).resolve().parents[2] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from config.settings import Settings, get_settings  # noqa: E402
from dagster import (  # noqa: E402
    AssetExecutionContext,
    Definitions,
    MonthlyPartitionsDefinition,
    PartitionsDefinition,
    WeeklyPartitionsDefinition,
    asset,
    define_asset_job,
)
from observability.structured import configure_logging  # noqa: E402
from scraping.runner import run_spider  # noqa: E402
from transformation.transformer import transform_range  # noqa: E402


def build_partitions(settings: Settings) -> PartitionsDefinition:
    """Dagster partitions mirror PARTITION_SIZE so one asset partition == one scrape partition."""
    start = settings.pipeline_start_date.isoformat()
    if settings.partition_size == "weekly":
        return WeeklyPartitionsDefinition(start_date=start)
    if settings.partition_size == "monthly":
        return MonthlyPartitionsDefinition(start_date=start)
    raise ValueError(f"unsupported partition size: {settings.partition_size}")


def partition_window(context: AssetExecutionContext) -> tuple[date, date]:
    """The asset's half-open [start, end) window, matching the scraper's date contract."""
    window = context.partition_time_window
    return window.start.date(), window.end.date()


_settings = get_settings()
_partitions = build_partitions(_settings)


@asset(
    partitions_def=_partitions,
    group_name="ingestion",
    description="Scrape every body for this partition into the landing bucket and collection.",
)
def landing_documents(context: AssetExecutionContext) -> None:
    configure_logging()
    start, end = partition_window(context)
    summary = run_spider(
        start, end, partition_size=_settings.partition_size, on_line=context.log.info
    )
    context.add_output_metadata(
        {
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "records_found": summary.records_found,
            "records_scraped": summary.records_scraped,
            "failed": summary.failed,
        }
    )
    if not summary.succeeded:
        raise RuntimeError(f"scrapy exited with code {summary.exit_code}")


@asset(
    partitions_def=_partitions,
    deps=[landing_documents],
    group_name="transformation",
    description="Read this partition's landing documents and write the transformed zone.",
)
def transformed_documents(context: AssetExecutionContext) -> None:
    configure_logging()
    start, end = partition_window(context)
    stats = transform_range(start, end, settings=_settings)
    context.add_output_metadata(
        {
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "records_found": stats.records_found,
            "records_transformed": stats.records_scraped,
            "failed": stats.failed,
        }
    )


pipeline_job = define_asset_job(
    name="workplace_relations_pipeline",
    selection=[landing_documents, transformed_documents],
    description="Scrape then transform one date partition.",
)

defs = Definitions(
    assets=[landing_documents, transformed_documents],
    jobs=[pipeline_job],
)
