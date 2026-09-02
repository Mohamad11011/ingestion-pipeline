# Workplace Relations Scraper — Implementation Requirements & Plan

This document converts the coding test into a complete implementation checklist and development plan.

**Company rules in this document are mandatory.** Implementation contracts below only define *how* those rules are satisfied. Do not drop, weaken, or replace a company requirement.

## Progress (where we reached)

Last updated: 2026-09-02. Repo root is `ingestion-pipeline` (not a nested `workplace-relations-scraper/` folder).

| Phase | Status | Notes |
|---|---|---|
| 0 Source recon | **DONE** | GET listing contract proven. Documented in Phase 0. |
| 1 Infrastructure | **DONE** | `docker-compose.yml` — Mongo + MinIO healthy; `minio-init` created `landing` and `transformed`. |
| 2 Core utilities | **DONE** | Config, date partitioner, SHA-256, metadata identity, storage ports, Mongo repository, MinIO client, structured JSON logging. |
| 3 Scraper | **DONE** | `src/scraping/{bodies,listing,ingest}.py` + spider/items/pipelines/middlewares. Runtime body discovery, pagination, JSON logs. Live: 45 Labour Court records for 2024-01. |
| 4 Idempotency | **DONE** | Re-run of the same range wrote 0 objects and created 0 duplicates. Source sends no ETag/Last-Modified, so SHA-256 comparison is the operative guard. |
| 5 Transformation | **DONE** | `transformation/html.py` (BeautifulSoup, `div.content` proven on a real saved page) + `transformer.py` (streamed partition range, `identifier.ext`, transformed bucket/collection) + CLI. |
| 6 Orchestration | NOT STARTED | `orchestration/dagster/` placeholder only. |
| 7 Tests | **PARTIAL** | 63 tests passing. Remaining gap is Phase 6 orchestration coverage. |
| 8 Documentation | **PARTIAL** | Short README + `.env.example`. `ARCHITECTURE.md` not written. |
| 9 End-to-end | NOT STARTED | |

**Next:** Phase 6 (Dagster assets calling `transform_range` + the spider), then Phase 8 `ARCHITECTURE.md` and Phase 9 end-to-end.

## 1. What You Are Building

Build two connected pipelines:

```text
Workplace Relations Website
          |
          v
       Scrapy
          |
     +----+----+
     |         |
     v         v
  MongoDB   Object Storage
 Metadata   Landing Zone
     |         |
     +----+----+
          |
          v
   Transformation
          |
     +----+----+
     |         |
     v         v
New Mongo   New Object
Collection  Storage Container
```

The Landing Zone must remain unchanged during transformation.

## 1.1 Company rules vs implementation contracts

The test asks for outcomes that must all be true at once. The contracts in this section are the chosen *how*. They do not add new product requirements and they do not override the checklist.

| Company rule (keep) | Implementation contract (how) |
|---|---|
| Use Scrapy | The spider, items, pipelines, middleware, and settings are Scrapy. Listing is a plain GET (no JS render). Do not replace Scrapy with a browser-only scraper. |
| Scrape every body on the site | Discover bodies from the site body filter (the left-side / checkbox list the test refers to). Iterate every discovered body. Do not hardcode a partial body list. |
| Accept `start_date` and `end_date`; partition the range | Partitions are half-open `[start, end)`. The example `end_date = 2025-01-01` with last partition `2024-12-01 -> 2025-01-01` is exclusive at `end_date`. `partition_date` is the partition start (`YYYY-MM-DD`). Dates are calendar dates in Europe/Dublin with no time component. |
| Configurable partition size | `PARTITION_SIZE=monthly` is the default. The partitioner must also support other sizes (for example `weekly`) from config without code edits. |
| Metadata includes `body + identifier` (or another deterministic key) | Record identity is `body` + `identifier`. Unique Mongo index on that pair. Upsert on that pair. |
| Store files in a Landing Zone; transformation must not change it | Transformation **only reads** landing storage and landing Mongo. It never puts, deletes, or overwrites landing objects or landing documents. Scraper re-runs are ingestion, not transformation. |
| Re-run the same range: no duplicate records; do not unnecessarily re-download unchanged files; detect changes | Before downloading, look up the identity in Mongo. Use HTTP validators (`HEAD` / `ETag` / `Last-Modified` / `Content-Length`) to skip the download when the source is unchanged. After a download, SHA-256 the bytes. If the hash matches the stored `file_hash`, do not re-put the object. If it differs, write the new landing object and upsert Mongo. That is the required hash comparison. |
| SHA-256 on every stored file | Hash the exact bytes written to object storage (landing hash of landing bytes; transformed hash of transformed bytes). |
| HTML: follow the page, scrape relevant content, store `.html` in landing | Landing stores the fetched decision page as `.html` (relevant page content, not a screenshot of the whole site chrome if it can be avoided). Transformation then strips remaining nav/buttons/header/footer as the test requires. Both steps stay. |
| PDF/DOC/DOCX: download as-is to landing; do not transform content | Landing stores original bytes. Transformation copies those bytes to the transformed bucket with no content change. |
| Every transformed file named `identifier.ext` | Transformed object key is the sanitized identifier plus the original extension, e.g. `ADJ-00012345.pdf`. Sanitization only replaces characters that are illegal in object keys; the identifier remains recognizable. Landing keys may include `body` so identities stay unique even when transformed keys cannot. |
| New object storage container and new Mongo collection for transformed output | Two buckets: `S3_LANDING_BUCKET` and `S3_TRANSFORMED_BUCKET`. Two collections: `MONGO_LANDING_COLLECTION` and `MONGO_TRANSFORMED_COLLECTION`. Do not use a single bucket with two prefixes as the primary design. |
| JSON logs: partition, body, found/scraped counts, failed URL + error, run summary | `records_found` = listing hits in that body×partition. `records_scraped` = records with metadata + landing file stored. `failed` = records that did not complete (download or parse). A failed document logs `url`, `error_code`, `reason` and does not abort the run. |
| Orchestrator (Dagster / Airflow / Modal) with scrape then transform | Separate tasks/assets. Transform depends on scrape. CLI remains documented. |
| Config not hardcoded | One settings module loaded from environment / `.env`. Scrapy settings read from that module. No duplicated magic values. |

Landing object key (ingestion):

```text
{body}/{sanitized_identifier}{ext}
```

Transformed object key (required by the test):

```text
{sanitized_identifier}{ext}
```

If two bodies share the same identifier, landing keys stay unique; transformed keys follow the test’s `identifier.ext` rule. Document that source limitation in `ARCHITECTURE.md`.

---

# 2. Scraper Requirements

## 2.1 Scrapy

- [x] Use the Scrapy framework.
- [x] Implement one or more Scrapy spiders.
- [x] Use proper Scrapy settings, pipelines, items/models, and middleware where appropriate.
- [x] Optimize for fast scraping without getting blocked.
- [x] Configure concurrency, delays, retries, and throttling appropriately.
- [x] Avoid unnecessary requests.
- [x] Handle pagination for every body × partition listing.
- [x] Send an identifying User-Agent and respect robots.txt / HTTP 429 (required to avoid getting blocked, not an extra product feature).
- [x] Before writing the spider, reproduce one body × one month search from Scrapy shell (or equivalent HTTP replay). Keep using Scrapy.

Pipelines must delegate persistence to shared storage/hash modules (single responsibility). Do not put Mongo and MinIO logic only inside `pipelines.py`.

## 2.2 Scrape All Bodies

The scraper must:

- [x] Scrape each body listed on the left side of the Workplace Relations website.
- [x] Iterate through every body.
- [x] Apply date filtering to each body.
- [x] Store the body associated with each record.
- [x] Handle listing pagination per body × partition.

Discover bodies from the live site body filter (left-side list / checkboxes). Bind the spider to those body values, not to a screenshot of the layout.

Conceptually:

```text
Body A
  -> Jan 2024
  -> Feb 2024
  -> Mar 2024

Body B
  -> Jan 2024
  -> Feb 2024
  -> Mar 2024

Body C
  -> ...
```

---

# 3. Date Partitioning

The scraper must accept:

```text
start_date
end_date
```

Example:

```text
start_date = 2024-01-01
end_date   = 2025-01-01
```

The scraper must iterate over the date range in partitions rather than treating the entire period as one scrape.

Date bounds contract:

```text
Partitions are half-open: [partition_start, partition_end)
end_date is exclusive
partition_date = partition_start as YYYY-MM-DD
timezone: Europe/Dublin, date-only
```

Example monthly partitioning:

```text
2024-01-01 -> 2024-02-01
2024-02-01 -> 2024-03-01
2024-03-01 -> 2024-04-01
...
2024-12-01 -> 2025-01-01
```

Requirements:

- [x] Accept start date.
- [x] Accept end date.
- [x] Generate date partitions.
- [x] Make partition size configurable.
- [x] Apply the partitions to each body.
- [x] Add `partition_date` to every record.

Empty partitions (zero listing hits) are valid: log counts of 0 and continue. They are not run failures.

---

# 4. Metadata Extraction

Extract metadata for every record.

At minimum:

```text
title
description
identifier
date
document_url
partition_date
body
```

Recommended additional metadata:

```text
source_url
document_type
scraped_at
http_etag
http_last_modified
```

`document_type` is taken from Content-Type plus file signature (magic bytes), not extension alone.

If a decision HTML page only wraps a PDF/DOC link, follow the document link for landing bytes (PDF/DOC rules). Keep `source_url` as the HTML page and `document_url` as the file URL.

Example:

```json
{
  "identifier": "...",
  "title": "...",
  "description": "...",
  "date": "...",
  "body": "...",
  "source_url": "...",
  "document_url": "...",
  "partition_date": "...",
  "file_path": "...",
  "file_hash": "..."
}
```

---

# 5. NoSQL Database

Use a NoSQL database.

Recommended choice:

```text
MongoDB
```

Requirements:

- [x] MongoDB.
- [x] Run MongoDB in Docker.
- [x] Create a landing metadata collection.
- [x] Store all scraped metadata.
- [x] Store file path.
- [x] Store file hash.
- [x] Add appropriate indexes.
- [x] Unique index on `body` + `identifier`.
- [x] Support idempotent/upsert behavior.

---

# 6. Object Storage

Use object/blob storage in Docker.

Recommended choice:

```text
MinIO
```

Two buckets (required: new transformed container, landing kept separate):

```text
S3_LANDING_BUCKET
    {body}/{sanitized_identifier}{ext}

S3_TRANSFORMED_BUCKET
    {sanitized_identifier}{ext}
```

Compose must create both buckets on first boot (init job). Do not treat `landing/` and `transformed/` prefixes inside one bucket as the primary design.

Requirements:

- [x] Object storage runs in Docker.
- [x] Create a Landing Zone.
- [x] Store downloaded documents in Landing Zone.
- [x] Keep Landing Zone immutable during transformation.

---

# 7. Document Downloading

## PDF / DOC / DOCX

If a record links directly to a PDF or document:

- [x] Download it.
- [x] Store it in Landing Zone.
- [x] Preserve the document as-is.
- [x] Calculate its file hash.
- [x] Store its path in MongoDB.
- [x] Store its hash in MongoDB.

The PDF/DOC/DOCX branch is implemented (magic-byte type detection, bytes stored verbatim,
no normalization) and unit-tested, but **not exercised against the live source**: every
listing hit encountered so far resolves to an HTML decision page.

## HTML

If a record links to an HTML page:

- [x] Navigate to the page.
- [x] Scrape the relevant page content.
- [x] Store the resulting page as `.html`.
- [x] Store its path in MongoDB.
- [x] Calculate and store its hash.

Landing HTML is the fetched decision page stored as `.html` (company rule). Transformation still removes remaining website UI (company rule). Landing is not the cleaned file; the transformed object is.

Landing object key:

```text
{body}/{sanitized_identifier}{ext}
```

---

# 8. File Hashing

Calculate a hash for every stored file.

Recommended:

```text
SHA-256
```

Store:

```json
{
  "file_hash": "..."
}
```

The hash is also required for idempotency and change detection.

Hash the bytes actually stored. Do not hash a URL string.

Change detection order (keeps “compare hash” and “do not unnecessarily re-download” both true):

```text
1. Lookup Mongo by body + identifier
2. If present, HEAD (or conditional GET) using stored ETag / Last-Modified
3. If validator unchanged -> skip download, keep landing object
4. If validator missing or changed -> download to temp
5. SHA-256 temp bytes
6. If hash equals stored file_hash -> skip object put
7. If hash differs -> put landing object, upsert Mongo file_path + file_hash
```

---

# 9. Idempotency

Running the same date range twice must not create duplicates or unnecessarily re-download unchanged files.

Expected behavior:

```text
Run #1
  -> scrape records
  -> download files
  -> store metadata

Run #2
  -> find existing records
  -> HTTP validator and/or file hash compare
  -> skip unchanged files (no unnecessary download or put)
  -> do not create duplicate records
  -> if source bytes changed: update landing object + landing metadata
```

Requirements:

- [x] Define deterministic record identity.
- [x] Use MongoDB unique indexes where appropriate.
- [x] Use upsert logic.
- [x] Compare file hashes.
- [x] Do not re-download unchanged files.
- [x] Detect changed files.
- [x] Update metadata appropriately when a source file changes.

Identity (locked):

```text
body + identifier
```

This is ingestion-time upsert. Transformation must not write the landing collection.

---

# 10. Structured JSON Logging

Logs must be structured JSON.

Each run should capture:

## Partition

```json
{
  "partition": "2024-01-01/2024-02-01"
}
```

## Body

```json
{
  "body": "..."
}
```

## Counts

```json
{
  "records_found": 200,
  "records_scraped": 195
}
```

Definitions:

```text
records_found     listing hits in this body × partition
records_scraped   metadata + landing file stored successfully
failed            found records that did not complete
```

`records_found` should equal `records_scraped + failed` for that scope (partition, body, or full run).

## Failed downloads

Include:

```json
{
  "url": "...",
  "error_code": "...",
  "reason": "..."
}
```

## Run summary

```json
{
  "event": "run_summary",
  "records_found": 1000,
  "records_scraped": 985,
  "failed": 15
}
```

Requirements:

- [x] JSON logs.
- [x] Current partition.
- [x] Current body.
- [x] Number of records found.
- [x] Number successfully scraped.
- [x] Failed download URLs.
- [x] Error codes/reasons.
- [x] End-of-run summary.

---

# 11. Infrastructure

Use Docker for infrastructure.

Minimum:

```text
docker-compose.yml

services:
  mongodb
  minio
  minio-init   # create landing + transformed buckets
```

Recommended environment:

```text
MongoDB
MinIO
Dagster
```

Dagster may run on the host against Compose services, or as an extra Compose service. Either is valid; document the chosen way in the README.

The infrastructure should be reproducible from a clean machine. Compose must wait until Mongo and MinIO are healthy before the scraper runs.

---

# 12. Orchestration

Use one of:

- [ ] Dagster
- [ ] Airflow
- [ ] Modal

Recommended:

```text
Dagster
```

The pipeline should separate ingestion and transformation into different tasks/assets.

Example:

```text
Scrape
  |
  v
Landing Zone
  |
  v
Transform
  |
  v
Transformed Zone
```

Requirements:

- [ ] Scraping task.
- [ ] Transformation task.
- [ ] Correct dependency handling.
- [ ] Transformation runs after required ingestion.
- [ ] Document CLI execution as well if useful.

Using an orchestrator carries significant weight in the assessment, so it is recommended rather than relying only on CLI execution.

---

# 13. Configuration

No infrastructure or scraping configuration should be hardcoded.

Configuration must be provided through environment variables or a config file.

At minimum configure:

```text
MongoDB connection string
MongoDB database
MongoDB collections
Object storage endpoint
Object storage credentials
Landing storage path/bucket
Transformed storage path/bucket
Partition size
Scraping concurrency
Scraping delay
Retry settings
Other scraping parameters
```

Example:

```env
MONGO_URI=...
MONGO_DATABASE=...
MONGO_LANDING_COLLECTION=...
MONGO_TRANSFORMED_COLLECTION=...

S3_ENDPOINT=...
S3_ACCESS_KEY=...
S3_SECRET_KEY=...
S3_LANDING_BUCKET=...
S3_TRANSFORMED_BUCKET=...

PARTITION_SIZE=monthly

SCRAPE_CONCURRENCY=...
SCRAPE_DELAY=...
SCRAPE_RETRY_TIMES=...
SCRAPE_USER_AGENT=...
```

Scrapy `settings.py` must import these values from `src/config/settings.py`. Do not maintain two independent configs.

Include:

```text
.env.example
```

Do not commit real secrets.

---

# 14. Transformation Pipeline

Create a Python transformation script.

Inputs:

```text
start_date
end_date
```

Process:

```text
start/end dates
      |
      v
MongoDB landing metadata
where partition_date >= start_date
  and partition_date <  end_date
      |
      v
Get landing file_path values
      |
      v
Landing object storage (READ ONLY)
      |
      v
Iterate those files (do not list the whole bucket)
```

Requirements:

- [x] Accept start date.
- [x] Accept end date.
- [x] Fetch metadata from MongoDB.
- [x] Fetch referenced files from object storage.
- [x] Iterate through the files.
- [x] Determine file type.
- [x] Apply the correct transformation behavior.

Filter transformation by `partition_date` using the same half-open range as the scraper. Do not load the entire landing collection into memory.

---

# 15. PDF / DOC Transformation

For PDF/DOC/DOCX:

```text
DO NOT TRANSFORM CONTENT
```

Requirements:

- [x] Leave PDF unchanged.
- [x] Leave DOC unchanged.
- [x] Leave DOCX unchanged.
- [x] Copy/store the file in transformed object storage.
- [x] Rename it to `identifier.ext`.
- [x] Calculate/store the transformed file hash.

---

# 16. HTML Transformation

For HTML:

Use an HTML parser such as:

```text
BeautifulSoup
```

The goal is to keep the relevant document content and remove website UI.

This is the required transform step. It runs on the landing `.html` file and writes a new object to the transformed bucket. It does not modify the landing `.html`.

Target the decision/content container with a CSS/XPath selector proven against a saved fixture of a real WRC page. Do not only strip by tag name globally.

Remove/exclude things such as:

```text
navigation bars
buttons
headers
footers
menus
other irrelevant website elements
```

Requirements:

- [x] Parse HTML.
- [x] Identify relevant document content.
- [x] Remove navigation.
- [x] Remove buttons.
- [x] Remove headers where they are website UI rather than document content.
- [x] Remove footers.
- [x] Remove other irrelevant UI.
- [x] Preserve relevant document content.
- [x] Produce cleaned `.html`.
- [x] Calculate the new file hash.

---

# 17. Rename ALL Transformed Files

Every transformed file must be named:

```text
identifier.ext
```

Sanitize only illegal object-key characters (path separators, control chars). Keep the identifier readable. Extension stays the real type (`.pdf`, `.doc`, `.docx`, `.html`).

Examples:

```text
ABC123.pdf
ABC124.docx
ABC125.html
```

Requirements:

- [x] Rename PDFs.
- [x] Rename DOC/DOCX.
- [x] Rename HTML.
- [x] Use the identifier from metadata.
- [x] Preserve the correct extension.

---

# 18. Transformed Object Storage

Do not overwrite Landing Zone files.

Create/use a separate object storage container/bucket:

```text
S3_LANDING_BUCKET
    {body}/{sanitized_identifier}{ext}

S3_TRANSFORMED_BUCKET
    {sanitized_identifier}.pdf
    {sanitized_identifier}.docx
    {sanitized_identifier}.html
```

Requirements:

- [x] New transformed storage container/bucket.
- [x] Store all transformed documents there.
- [x] Never modify Landing Zone files.
- [x] Never delete Landing Zone files.

Those last two apply to the transformation job. The scraper may upsert landing objects on a changed source file.

---

# 19. Transformed MongoDB Collection

Create a new NoSQL collection for transformed metadata.

For example:

```text
landing.metadata
transformed.metadata
```

The transformed record should contain the updated information. Preserve landing metadata fields and overwrite storage fields:

```json
{
  "identifier": "...",
  "title": "...",
  "description": "...",
  "date": "...",
  "body": "...",
  "source_url": "...",
  "document_url": "...",
  "document_type": "...",
  "partition_date": "...",
  "file_path": "...",
  "file_hash": "..."
}
```

`file_path` is the transformed bucket key (the required `identifier.ext`). `file_hash` is SHA-256 of the transformed bytes.

Upsert transformed Mongo on the same identity (`body` + `identifier`). Do not write to the landing collection.

Requirements:

- [x] New MongoDB collection.
- [x] Preserve metadata.
- [x] Store new file path.
- [x] Store new file hash.
- [x] Do not modify landing metadata unnecessarily.

---

# 20. Architecture.md

Create:

```text
ARCHITECTURE.md
```

Maximum:

```text
1 page
```

It MUST explain:

## 20.1 Date partition size

Explain why you selected the partition size.

For example:

```text
Monthly partitions balance request volume,
retry isolation, operational visibility, and
resource usage.
```

Explain your actual choice.

Include the half-open `[start, end)` rule and `partition_date` = partition start.

## 20.2 Retries and rate limiting

Explain:

- retry strategy
- HTTP error handling
- timeouts
- concurrency
- delays
- throttling
- handling HTTP 429
- how you avoid getting blocked

## 20.3 Deduplication

Explain:

- record identity
- MongoDB uniqueness
- upserts
- file hash comparison
- HTTP validators used to avoid unnecessary downloads
- unchanged-file handling
- changed-file handling

State clearly: transformation never writes landing; scraper upserts landing on change.

## 20.4 Scaling to 50+ sources

Explain how you would evolve the system from one source to many.

Possible direction:

```text
Source configuration
       |
       v
Generic scraping framework
       |
       +--> Source adapter A
       +--> Source adapter B
       +--> Source adapter C
       |
       v
Common metadata schema
       |
       v
Shared storage layer
```

---

# 21. README.md

Create a complete:

```text
README.md
```

It should explain:

```text
# Project

## Architecture

## Requirements

## Setup

## Docker

## Environment Variables

## Starting Infrastructure

## Running the Scraper

## Running Transformation

## Running Full Pipeline

## Running Tests

## Example Commands

## MongoDB Structure

## Object Storage Structure

## Logging

## Idempotency

## Error Handling

## Design Decisions
```

The README must provide clear instructions for running the solution.

---

# 22. Error Handling

Implement robust error and exception handling.

Handle at least:

```text
HTTP 404
HTTP 403
HTTP 429
HTTP 500
connection failures
timeouts
invalid URLs
download failures
MongoDB failures
object storage failures
malformed HTML
missing metadata
unexpected file types
```

Important:

A single failed document should not terminate the complete scraping run.

Example:

```text
Website records: 200

Successful: 193
Failed:       7
Total:      200
```

Every failed record must be logged with:

```text
URL
error code
reason
```

HTTP 429: back off and retry within the partition; if still failing, log that document/request and continue. Do not abort the whole run.

Missing identifier or body: log and skip that record; do not invent an identity.

---

# 23. Scalability

The assessment will evaluate approximately:

```text
500–1000 documents
```

But the design should be capable of approximately:

```text
1000x that volume
```

Avoid loading the entire dataset into memory.

Prefer:

- streaming
- batching
- pagination
- efficient MongoDB operations
- indexes
- bounded concurrency
- efficient object storage operations

Write each record to Mongo and object storage as it is scraped. Do not collect a full body×partition in memory.

---

# 24. Landing Zone Immutability

The Landing Zone is the raw/source layer for transformation.

Architecture:

```text
Landing Zone
     |
     | READ ONLY (transformation)
     v
Transformation
     |
     v
Transformed Zone
```

Transformation must never:

- [x] Delete landing files.
- [x] Modify landing files.
- [x] Transform files in-place.
- [x] Overwrite raw data.

Scraper re-runs may upsert landing objects when the source file hash changes. That is ingestion idempotency, not transformation.

End-to-end check “Landing Zone remains untouched” means: after a transform run, landing bytes and landing Mongo documents are identical to before that transform run.

---

# 25. Python Code Quality

Requirements:

- [ ] Type hints.
- [ ] Clear naming.
- [ ] Small focused functions.
- [ ] Useful docstrings.
- [ ] Proper exception handling.
- [ ] No giant monolithic scripts.
- [ ] No unnecessary duplicated code.
- [ ] Configuration separated from business logic.
- [ ] Structured logging instead of `print`.
- [ ] Dependency management.
- [ ] Readable code.
- [ ] Scalable design.
- [ ] Python best practices.

---

# 26. Recommended Project Structure

```text
workplace-relations-scraper/
│
├── README.md
├── ARCHITECTURE.md
├── docker-compose.yml
├── .env.example
├── pyproject.toml
├── Makefile
│
├── scrapy_project/
│   ├── scrapy.cfg
│   └── workplace_scraper/
│       ├── spiders/
│       │   └── workplace.py
│       ├── items.py
│       ├── pipelines.py      # delegates to src.storage / src.hashing
│       ├── middlewares.py
│       └── settings.py       # reads src.config.settings
│
├── src/
│   ├── config/
│   │   └── settings.py
│   │
│   ├── storage/
│   │   ├── mongo.py
│   │   └── object_storage.py
│   │
│   ├── models/
│   │   └── metadata.py
│   │
│   ├── partitioning/
│   │   └── dates.py
│   │
│   ├── hashing/
│   │   └── files.py
│   │
│   ├── transformation/
│   │   ├── transformer.py
│   │   └── html.py
│   │
│   └── logging/
│       └── structured.py
│
├── orchestration/
│   └── dagster/
│
└── tests/
    ├── fixtures/
    │   └── sample_decision.html
    ├── test_partitioning.py
    ├── test_hashing.py
    ├── test_html_transform.py
    ├── test_idempotency.py
    └── test_storage.py
```

This is a recommendation, not a strict requirement. Keep Scrapy as the scraper. Keep one metadata model; Scrapy items map into it.

---

# 27. Testing Plan

Add tests even though a specific testing framework is not mandated.

Recommended:

```text
tests/
├── fixtures/
│   └── sample_decision.html
├── test_partitioning.py
├── test_hashing.py
├── test_html_transform.py
├── test_idempotency.py
└── test_storage.py
```

## Date partitioning

Verify:

```text
2024-01-01 -> 2024-04-01
```

produces monthly partitions `[2024-01-01, 2024-02-01)`, `[2024-02-01, 2024-03-01)`, `[2024-03-01, 2024-04-01)` and does not include 2024-04-01 as a start.

## Idempotency

Run the same range twice and verify:

```text
No duplicate Mongo records
No unnecessary file downloads
```

## Hashing

Verify:

```text
same file    -> same hash
changed file -> different hash
```

## HTML

Verify navigation/header/footer/UI elements are removed while relevant document content remains.

---

# 28. Development Order

Follow this order to avoid building everything at once. Company requirements stay in force in every phase.

## Phase 0 — Source recon (still Scrapy) — DONE

Reproduce one body × one month listing over HTTP. Do not start the full spider until this request contract works.

**Status:** proven with stdlib HTTP. Scrapy is installed; it will issue the same GET. No browser, no ViewState, no captcha for search.

### Request contract

Listing (one body × date range, page 1):

```text
GET https://www.workplacerelations.ie/en/search/
    ?decisions=1
    &from={d/m/yyyy}
    &to={d/m/yyyy}
    &body={body_id}
    &pageNumber={n}          # optional; default 1
```

Proven examples:

```text
WRC, 1 Jan–1 Feb 2024:
  /en/search/?decisions=1&from=1/1/2024&to=1/2/2024&body=15376
  -> "of 246 results", 10 items per page

Labour Court, same dates:
  /en/search/?decisions=1&from=1/1/2024&to=1/2/2024&body=3
  -> "of 47 results", same card markup
```

An ASP.NET POST with ViewState also works and redirects to this GET. Use GET only.

Site `from` / `to` are **inclusive calendar days** (`d/m/yyyy`). Our partitions stay half-open `[start, end)`. Convert:

```text
from = partition_start as d/m/yyyy
to   = (partition_end - 1 day) as d/m/yyyy
```

Example: partition `[2024-01-01, 2024-02-01)` → `from=1/1/2024&to=31/1/2024`. Passing `to=1/2/2024` incorrectly includes 1 February.

### Bodies (left-side checkboxes)

Discover at runtime from `#CB2_0`…`#CB2_3` on `/en/search/?advance=true`. Current values:

```text
1     Equality Tribunal
2     Employment Appeals Tribunal
3     Labour Court
15376 Workplace Relations Commission
```

`CB1_*` is case type (Appeal / Complaint / …), not body. Do not use it to iterate bodies.

### Listing card

`li.each-item`:

```text
h2.title a[href]     identifier + document/source URL
span.date            dd/mm/yyyy  (decision date)
p.description        title / parties
span.refNO           identifier
```

Identifiers seen: `ADJ-00039955` (WRC), `LCR22916`, `RPD241`, `PWD246` (Labour Court).

### Pagination

```text
page size     10
total         parse "of {N} results"
next page     ul.pager a.next  (page 10 still has next -> page 11)
stop          when a.next is absent or the page has zero li.each-item
```

Do not trust the highest visible page number (the pager window shows ~10 links).

### Document pages

Follow `a[href]` on the card. Observed:

```text
https://www.workplacerelations.ie/en/cases/2024/february/adj-00039955.html
https://www.workplacerelations.ie/en/cases/2024/february/lcr22916.html
```

These are HTML decision pages (full text on the page: Parties, Complaint, Background, …). Listing pages did not link directly to PDFs. The downloader must still detect PDF/DOC if a later body page links to a file.

### robots.txt

```text
Allow:   /en/search/
Disallow /en/Cases/   (decision HTML lives here)
```

The test requires those public decision pages. Use an identifying User-Agent, delays, and 429 backoff. Do not hit disallowed import trees (`/EAT_Import/`, `/Labour_Court_Import/`, …).

### Selectors for the spider

```text
bodies     #CB2_0, #CB2_1, #CB2_2, #CB2_3  (value + label)
results    li.each-item
next       ul.pager a.next::attr(href)
total      text "of {N} results"
```

## Phase 1 — Infrastructure — DONE

Verified 2026-09-02: `wr-mongodb` healthy on 27017, `wr-minio` healthy on 9000/9001, buckets `landing` and `transformed` created.

```text
Docker Compose
    |
    +--> MongoDB
    |
    +--> MinIO
    |
    +--> bucket init (landing + transformed)
```

## Phase 2 — Core utilities (with unit tests) — DONE

```text
[x] Configuration          src/config/settings.py
[x] Date partitioner       src/partitioning/dates.py + tests
[x] File hashing           src/hashing/files.py + tests
[x] Metadata model         src/models/metadata.py + tests
[x] Storage ports          src/storage/ports.py
[x] Mongo repository       src/storage/mongo.py + tests
[x] Object storage client  src/storage/object_storage.py + tests
[x] Structured logging     src/observability/structured.py + tests
```

`MongoRepository` owns the identity index (`uniq_body_identifier`), the `partition_date`
index, landing/transformed upserts, and a batched half-open partition-range cursor for
Phase 5. `S3ObjectStorage` is path-style S3v4 (MinIO). `structured.py` provides the JSON
formatter, `log_failure(url, error_code, reason)`, and `RunStats`/`ScopeStats` so that
`records_found == records_scraped + failed` per body x partition and per run.

## Phase 3 — Scraper — DONE

Implement:

```text
Discover bodies
      |
Generate date partitions
      |
Scrape records (paginated)
      |
Extract metadata
      |
Download files (HEAD/hash skip)
      |
Hash files
      |
Mongo + Landing Storage
```

## Phase 4 — Idempotency — DONE

Test:

```text
Run #1
  -> records/files

Run #2
  -> no duplicates
  -> no unnecessary downloads
  -> changed files detected
```

Measured (Labour Court, 2024-01-29 -> 2024-02-01):

| Run | found/scraped/failed | objects written | unchanged by hash | Mongo |
|---|---|---|---|---|
| 1 | 8 / 8 / 0 | 8 | 0 | 8 inserted |
| 2 (same range) | 8 / 8 / 0 | **0** | **8** | 8 docs, 0 duplicates |
| 3 (full month) | 45 / 45 / 0 | 37 | 8 | 45 docs, 5 pages paginated |

Two source facts constrain this (both to go in `ARCHITECTURE.md`):

1. Decision pages send **no `ETag` and no `Last-Modified`**. Conditional GET is implemented,
   stored and unit-tested, but never fires against this source, so steps 2-3 of section 8 are
   inert here and the SHA-256 comparison is what prevents the re-put. `Content-Length`
   disagrees with the real body length by 1 byte and is deliberately not used as a validator.
2. Pages carry a volatile `<!-- Elapsed time: ... -->` render comment - the only byte that
   differs between two fetches of the same decision. It is normalized out of HTML payloads
   *before* storing and hashing (11 bytes on a sample page), so `file_hash` remains the hash
   of the exact stored bytes. Without this, run #2 rewrites every object. This is the only
   content change the landing zone performs and it applies to HTML only.

## Phase 5 — Transformation — DONE

Implement:

```text
Mongo landing metadata (partition_date filter)
      |
Get files (read-only)
      |
+-----+------+
|            |
PDF/DOC      HTML
unchanged    clean content
|            |
+-----+------+
      |
rename identifier.ext
      |
calculate hash
      |
new object storage bucket
      |
new Mongo collection
```

Confirm landing bytes unchanged after this phase.

Verified: landing objects (bytes + ETag + LastModified) and landing Mongo documents
were byte-identical before and after a transform run. `transformer.py` has no landing
write path at all — it only calls `storage.get(landing_bucket, ...)` and `upsert_transformed`.
CLI: `PYTHONPATH=src python -m transformation.transformer --start-date ... --end-date ...`
(`--end-date` exclusive). `transform_range(start, end)` is the callable for Dagster in Phase 6.

## Phase 6 — Orchestration

Put scraping and transformation into Dagster/Airflow/Modal with correct dependencies.

## Phase 7 — Tests

Focus on:

```text
Partitioning
Hashing
HTML cleaning (fixture)
Idempotency
Failure handling
```

## Phase 8 — Documentation

Finish:

```text
README.md
ARCHITECTURE.md
.env.example
```

## Phase 9 — End-to-End Validation

Run against the expected 500–1000 document scale.

Verify:

- [ ] Correct record counts.
- [ ] Every failure is logged.
- [ ] All successful files exist.
- [ ] Mongo metadata matches storage.
- [ ] Hashes are correct.
- [ ] Rerunning is idempotent.
- [ ] Changed files are detected.
- [ ] Landing Zone remains untouched **by transformation**.
- [ ] Transformation produces correctly named files.
- [ ] Transformed Mongo collection is correct.
- [ ] Transformed storage is correct.

---

# 29. Final Submission Checklist

## Scraping

- [x] Scrapy used.
- [x] All bodies scraped.
- [x] Start date accepted.
- [x] End date accepted.
- [x] Date range partitioned.
- [x] Partition size configurable.
- [x] `partition_date` stored.
- [x] Fast scraping strategy.
- [x] Rate limiting.
- [x] Retry handling.
- [x] Pagination handled.
- [x] Metadata extracted.
- [x] PDF downloaded.
- [x] DOC/DOCX downloaded.
- [x] HTML pages followed.
- [x] HTML stored as `.html`.

## Metadata

- [x] Title.
- [x] Description.
- [x] Identifier.
- [x] Date.
- [x] Document URL.
- [x] Body.
- [x] Partition date.
- [x] File path.
- [x] File hash.

## Landing Infrastructure

- [x] MongoDB.
- [x] MongoDB in Docker.
- [x] Object storage.
- [x] Object storage in Docker.
- [x] Landing Zone.
- [x] No hardcoded configuration.

## Idempotency

- [x] Unique record identity.
- [x] Mongo unique/index strategy.
- [x] Existing records detected.
- [x] File hash compared.
- [x] Unchanged files not re-downloaded.
- [x] No duplicate records.

## Logging

- [x] JSON logs.
- [x] Partition logged.
- [x] Body logged.
- [x] Records found.
- [x] Records successfully scraped.
- [x] Failed downloads.
- [x] Failed URL.
- [x] Error code/reason.
- [x] End-of-run summary.

## Orchestration

- [ ] Dagster/Airflow/Modal.
- [ ] Scraping task.
- [ ] Transformation task.
- [ ] Dependency handling.
- [ ] CLI execution documented if applicable.

## Transformation

- [x] Start date.
- [x] End date.
- [x] Fetch metadata from Mongo.
- [x] Fetch files from object storage.
- [x] Iterate through files.
- [x] PDF unchanged.
- [x] DOC unchanged.
- [x] DOCX unchanged.
- [x] HTML parsed.
- [x] Navigation removed.
- [x] Buttons removed.
- [x] Headers handled.
- [x] Footers removed.
- [x] Relevant content retained.
- [x] New hash calculated.
- [x] ALL files renamed to `identifier.ext`.
- [x] New object storage container/bucket.
- [x] New Mongo collection.
- [x] New file path stored.
- [x] New file hash stored.
- [x] Landing Zone untouched.

## Deliverables

- [ ] Git repository.
- [x] README.md.
- [ ] ARCHITECTURE.md (maximum 1 page).
- [x] Docker setup.
- [x] `.env.example`.
- [x] Tests.
- [ ] Clean code.
- [ ] Python best practices.
- [x] Robust error handling.

---

# 30. Key Assessment Mindset

Do not optimize only for "making the scraper work."

The assessment explicitly emphasizes:

```text
Scalability
Idempotency
Orchestration
Observability
Configuration
Error handling
Data quality
Python best practices
```

You must also be able to explain every major design decision, trade-off, and piece of code during the technical interview. Use section 1.1 as the short list of those decisions.

The strongest submission is therefore a relatively small but production-style pipeline rather than one large scraper script.
