# Workplace Relations scraper

Ingestion and transformation pipeline for the [Workplace Relations](https://www.workplacerelations.ie)
decisions database. Scrapy scrapes every body over configurable date partitions into a landing zone
(MongoDB + MinIO); a separate transformation step reads that landing zone read-only and writes a
cleaned, renamed copy into a second bucket and collection. Both steps are Dagster assets.

Test requirements are tracked in
[`docs/Workplace_Relations_Coding_Test_Requirements_Plan.md`](docs/Workplace_Relations_Coding_Test_Requirements_Plan.md).
Design rationale is in [`ARCHITECTURE.md`](ARCHITECTURE.md).

## Architecture

```
workplacerelations.ie
        |
      Scrapy  (bodies x date partitions -> paginated listings -> documents)
        |
   +----+----------------------+
   v                           v
Mongo landing.metadata     MinIO landing/{body}/{identifier}.ext
   |                           |
   +----------+----------------+
              |  READ ONLY
              v
        Transformation  (HTML cleaned, PDF/DOC/DOCX byte-identical)
              |
   +----------+----------------+
   v                           v
Mongo transformed.metadata  MinIO transformed/{identifier}.ext
```

| Area | Module |
|---|---|
| Configuration | `src/config/settings.py` (pydantic-settings, `.env`) |
| Date partitions | `src/partitioning/dates.py` |
| Metadata model | `src/models/metadata.py`, key/type rules in `src/models/documents.py` |
| Hashing | `src/hashing/files.py` |
| Storage | `src/storage/{mongo,object_storage,ports}.py` |
| Scraping | `scrapy_project/` + `src/scraping/{bodies,listing,ingest,runner}.py` |
| Transformation | `src/transformation/{transformer,html}.py` |
| Logging | `src/observability/structured.py` |
| Orchestration | `orchestration/dagster/definitions.py` |

## Requirements

- Python 3.11+
- Docker + Docker Compose (MongoDB, MinIO)
- Dagster for orchestration (`pip install -e ".[dev,orchestration]"`)

## Setup

```powershell
copy .env.example .env
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev,orchestration]"
```

On Linux/macOS use `cp .env.example .env` and `source .venv/bin/activate`.
Do not commit `.env`.

## Docker

`docker-compose.yml` runs three services:

| Service | Purpose | Ports |
|---|---|---|
| `mongodb` | metadata (landing + transformed), healthchecked | 27017 |
| `minio` | object storage (landing + transformed buckets), healthchecked | 9000 API, 9001 console |
| `minio-init` | creates the `landing` and `transformed` buckets once MinIO is healthy | -- |

Dagster runs on the host against these services (see *Running Full Pipeline*).

## Environment Variables

All configuration is environment-driven; nothing is hardcoded. Scrapy's `settings.py` imports the
same settings module, so there is only one config.

| Variable | Default | Meaning |
|---|---|---|
| `MONGO_URI` | `mongodb://root:changeme@localhost:27017/?authSource=admin` | connection string |
| `MONGO_DATABASE` | `workplace_relations` | database |
| `MONGO_LANDING_COLLECTION` | `landing.metadata` | landing metadata |
| `MONGO_TRANSFORMED_COLLECTION` | `transformed.metadata` | transformed metadata |
| `S3_ENDPOINT` | `http://localhost:9000` | MinIO S3 API |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | `minioadmin` | credentials |
| `S3_LANDING_BUCKET` | `landing` | landing zone bucket |
| `S3_TRANSFORMED_BUCKET` | `transformed` | transformed bucket |
| `S3_REGION` | `us-east-1` | signing region |
| `PARTITION_SIZE` | `monthly` | `monthly` or `weekly` |
| `PIPELINE_START_DATE` | `2024-01-01` | first Dagster partition |
| `SCRAPE_CONCURRENCY` | `8` | `CONCURRENT_REQUESTS` |
| `SCRAPE_DELAY` | `1.0` | `DOWNLOAD_DELAY` / AutoThrottle start delay |
| `SCRAPE_RETRY_TIMES` | `3` | `RETRY_TIMES` |
| `SCRAPE_USER_AGENT` | identifying UA | `User-Agent` header |

`MONGO_ROOT_USER`, `MONGO_ROOT_PASSWORD`, `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD` are consumed by
Compose. See `.env.example`.

## Starting Infrastructure

```bash
make up      # docker compose up -d, then ps
make logs
make down
```

Or directly: `docker compose --env-file .env up -d`. Compose waits for the Mongo and MinIO
healthchecks before `minio-init` creates the buckets, so a clean machine is reproducible in one
command.

## Running the Scraper

```bash
make scrape START_DATE=2024-01-01 END_DATE=2025-01-01
```

Equivalent:

```bash
cd scrapy_project
PYTHONPATH=../src python -m scrapy crawl workplace -a start_date=2024-01-01 -a end_date=2025-01-01
```

Spider arguments: `start_date`, `end_date` (**exclusive**), optional `partition_size` (overrides
`PARTITION_SIZE`), optional `body_ids` (comma-separated site body ids, for a narrow re-run). Bodies
are otherwise discovered from the site's own filter checkboxes at runtime.

## Running Transformation

```bash
make transform START_DATE=2024-01-01 END_DATE=2025-01-01
```

Equivalent:

```bash
PYTHONPATH=src python -m transformation.transformer --start-date 2024-01-01 --end-date 2025-01-01
```

Selects landing metadata by `start <= partition_date < end` with a batched cursor, fetches only
those objects, and writes the transformed bucket and collection. It never writes landing.

## Running Full Pipeline

```bash
make dagster     # DAGSTER_HOME=./.dagster dagster dev -f orchestration/dagster/definitions.py
```

Open <http://localhost:3000>. Two date-partitioned assets:

- `landing_documents` -- runs the spider for the partition's `[start, end)` window
- `transformed_documents` -- depends on `landing_documents`, runs `transform_range` for the same window

Materialize a partition (or a backfill over several) and Dagster runs scrape then transform in
order. The `workplace_relations_pipeline` job selects both. Partition granularity follows
`PARTITION_SIZE`, so one asset partition is exactly one scraper partition. The spider runs in a
subprocess because Twisted's reactor cannot be restarted in-process across materializations.

CLI execution (above) remains fully supported and is what the assets call.

## Running Tests

```bash
make test        # python -m pytest
python -m pytest --cov=src
```

Unit tests cover partitioning, hashing, identity/keys, metadata, settings, structured logging,
storage (mocked Mongo/S3), spider parsing, HTML cleaning against a real saved decision page,
idempotency, transformation, and the orchestration assets. Orchestration tests skip if Dagster is
not installed.

## Example Commands

```bash
make up
make scrape START_DATE=2024-01-01 END_DATE=2024-02-01
make scrape START_DATE=2024-01-01 END_DATE=2024-02-01   # re-run: 0 objects written, 0 duplicates
make transform START_DATE=2024-01-01 END_DATE=2024-02-01
make test
make dagster
```

Weekly partitions: `PARTITION_SIZE=weekly make scrape START_DATE=2024-01-01 END_DATE=2024-02-01`.
Single body: append `-a body_ids=3` (Labour Court) to the Scrapy command.

## MongoDB Structure

Two collections, both with a unique index on `body + identifier` (`uniq_body_identifier`) and an
index on `partition_date`.

`landing.metadata`:

```json
{
  "identifier": "ADJ-00039955",
  "title": "...",
  "description": "...",
  "date": "31/01/2024",
  "body": "Workplace Relations Commission",
  "source_url": "https://www.workplacerelations.ie/en/cases/2024/february/adj-00039955.html",
  "document_url": "https://www.workplacerelations.ie/en/cases/2024/february/adj-00039955.html",
  "document_type": "html",
  "partition_date": "2024-01-01",
  "file_path": "Workplace_Relations_Commission/ADJ-00039955.html",
  "file_hash": "<sha256 of the stored landing bytes>",
  "scraped_at": "2026-09-02T00:00:00+00:00",
  "http_etag": null,
  "http_last_modified": null
}
```

`transformed.metadata` preserves those fields and overwrites the storage fields: `file_path` becomes
the `identifier.ext` key, `file_hash` becomes the SHA-256 of the transformed bytes, plus
`source_file_path` and `transformed_at`.

## Object Storage Structure

```
landing/
    {body}/{identifier}.html|.pdf|.doc|.docx      # body prefix keeps identities unique
transformed/
    {identifier}.html|.pdf|.doc|.docx             # required by the test
```

Only characters illegal in an object key are sanitized; the identifier stays readable. The extension
reflects the detected type (magic bytes first, `Content-Type` second), not the URL.

## Logging

All logs are single-line JSON on stdout (`src/observability/structured.py`).

| Event | Fields |
|---|---|
| `run_started` | `start_date`, `end_date`, `partition_size`, `partitions` |
| `bodies_discovered` | `bodies` |
| `partition_started` | `body`, `partition`, `total_results` |
| `record_stored` | `body`, `identifier`, `partition`, `file_path`, `file_hash`, `document_type`, `object_written`, `inserted` |
| `document_unchanged` | `url`, `identifier`, `body`, `reason` |
| `rate_limited` | `url`, `error_code`, `retry_after` |
| `record_failed` | `url`, `error_code`, `reason` (+ `body`, `partition`, `identifier`) |
| `partition_summary` | `body`, `partition`, `records_found`, `records_scraped`, `failed` |
| `run_summary` | `records_found`, `records_scraped`, `failed` |

`records_found == records_scraped + failed` holds per body x partition and for the run.
Transformation emits the same shapes (`transform_started`, `document_transformed`,
`partition_summary`, `run_summary`).

## Idempotency

Record identity is `body + identifier`, enforced by a unique Mongo index and used as the upsert
filter, so re-running a range cannot create duplicates. Before downloading, the stored
`ETag`/`Last-Modified` are replayed as conditional headers and a `304` skips the download. Bytes
that are downloaded are SHA-256 hashed and compared with the stored `file_hash`: equal means the
object is not re-written; different means a new landing object and a metadata upsert.

Measured on Labour Court:

| Run | found / scraped / failed | objects written | unchanged by hash | Mongo |
|---|---|---|---|---|
| 1 (2024-01-29 to 2024-02-01) | 8 / 8 / 0 | 8 | 0 | 8 inserted |
| 2 (same range) | 8 / 8 / 0 | **0** | **8** | 8 docs, 0 duplicates |
| 3 (full month) | 45 / 45 / 0 | 37 | 8 | 45 docs, 5 pages paginated |

This source sends no `ETag`/`Last-Modified`, so the hash comparison is the operative guard. Two
volatile render comments (`<!-- Elapsed time: ... -->` and `<!-- cached or not being index.aspx
page -->`, the latter depending on the server's cache state) are normalized out before storing and
hashing; without that, a re-run rewrites nearly every object. Note that a repeated source reference
can collapse two documents into one record. See `ARCHITECTURE.md`.

## Error Handling

A single failed document never terminates a run. Every failure is logged as `record_failed` with
`url`, `error_code` and `reason`, counted into `failed`, and the crawl continues.

| Case | Handling |
|---|---|
| 404 / 403 / 500 | errback logs the status and reason; record counted as failed |
| 429 / 503 | logged with `Retry-After`, retried with AutoThrottle backoff, failed if still refused |
| timeouts, DNS, connection failures | classified (`timeout`, `dns_lookup_error`) and logged |
| invalid or missing document URL | `missing_metadata`, record skipped -- no invented identity |
| missing identifier or body | same; never fabricated |
| Mongo / object storage failure | item dropped with `landing write failed`, run continues |
| malformed HTML with no content container | `ContentNotFound`; that document fails transformation only |
| unexpected file type | magic-byte detection; unknown falls back to HTML handling |

## Design Decisions

- **Half-open partitions.** `[start, end)` with `end_date` exclusive; the site's inclusive
  `from`/`to` filters get `partition_end - 1 day`.
- **Monthly default, configurable.** Cheap retries and readable summaries; `weekly` supported.
- **Bodies discovered at runtime** from the site's filter checkboxes, never hardcoded.
- **Two buckets and two collections**, not one bucket with prefixes, so the landing zone is
  structurally separate from transformed output.
- **Transformation is read-only over landing** -- `transformer.py` has no landing write path.
- **Persistence lives in `src/storage` and `src/scraping/ingest.py`**; Scrapy pipelines only
  delegate, so the same code serves the CLI, the tests and Dagster.
- **Streaming everywhere** -- records are written as they are scraped, and transformation walks a
  batched partition-range cursor rather than loading the collection.
- **`ROBOTSTXT_OBEY=False` is deliberate**: the decision pages the test requires are under a
  disallowed path, so it is compensated with an identifying UA, delays, AutoThrottle and 429
  backoff, and the disallowed bulk import trees are never requested.
