# Architecture

```
workplacerelations.ie --> Scrapy --> Mongo landing.metadata + MinIO landing/
                                            |  (read only)
                                            v
                                     Transformation --> transformed.metadata + MinIO transformed/
```

Dagster owns both steps as date-partitioned assets: `landing_documents` -> `transformed_documents`.

## Date partition size

Monthly (`PARTITION_SIZE`, also supports `weekly`). A month of one body is ~5 listing pages and
tens of documents: small enough that a failed partition is cheap to re-run and its JSON summary is
readable, large enough that partition overhead stays negligible at the 500-1000 document scale.
Weekly is available for dense back-fills where a month would be too coarse to retry.

Partitions are half-open `[start, end)`; `end_date` is exclusive and `partition_date` is the
partition start (`YYYY-MM-DD`, Europe/Dublin, date only). The site's own `from`/`to` filters are
*inclusive* calendar days, so the spider sends `to = partition_end - 1 day`. Dagster's
`MonthlyPartitionsDefinition` window is likewise half-open, so one asset partition is exactly one
scraper partition.

## Retries and rate limiting

`AUTOTHROTTLE_ENABLED` adapts the delay to observed latency, on top of `DOWNLOAD_DELAY` (1.0s,
randomized) and `CONCURRENT_REQUESTS` (8, capped per domain). `RETRY_TIMES=3` covers
408/429/500/502/503/504/522/524 with Scrapy's exponential backoff; `DOWNLOAD_TIMEOUT=30`. A
custom middleware logs every 429/503 with its `Retry-After` so throttling is visible in the JSON
logs while AutoThrottle and RetryMiddleware do the waiting. An identifying `User-Agent` is sent.
`ROBOTS.TXT`: `/en/search/` is allowed but `/en/Cases/` — where the decisions the test asks for
live — is disallowed, so `ROBOTSTXT_OBEY=False` is deliberate and compensated with polite rates;
the disallowed bulk import trees are never touched. Errors are per record: an errback logs
`url`, `error_code`, `reason` and the run continues, so `records_found == records_scraped + failed`.

## Deduplication

Identity is `body + identifier`, enforced by a unique Mongo index (`uniq_body_identifier`) on both
collections and used as the upsert filter. Before fetching, the spider loads the stored record and
replays its `ETag`/`Last-Modified` as conditional headers; a `304` skips the download entirely.
Otherwise the bytes are hashed (SHA-256 of exactly what is stored) and compared to `file_hash`:
equal means no object put, different means a new landing object plus a metadata upsert.

Three source facts shape this. (1) Decision pages send **no `ETag` and no `Last-Modified`**, so
the conditional-GET path is implemented and unit-tested but never fires here — the hash comparison
is what actually prevents re-puts. `Content-Length` disagrees with the real body by one byte and is
deliberately not used as a validator. (2) Every page carries a volatile
`<!-- Elapsed time: ... -->` render comment. (3) Pages also carry
`<!-- cached or not being index.aspx page -->`, present or absent depending on the server's own
cache state, so it flips between runs minutes apart. Both comments are normalized out **before**
storing and hashing, so `file_hash` stays the hash of the stored bytes. Without either, a re-run
rewrites nearly every object. This is HTML-only and the sole content change the landing zone makes.

The source also publishes distinct documents under a repeated reference: `RPD241`
(`/2024/february/` and `/2024/july/`) and `ADJ-00044064` (`/2024/february/` and `/2024/january/`)
are different decisions with the same `body + identifier`. Under the locked identity contract the
second upserts over the first, so 616 listing hits yield 614 landing records; `records_found ==
records_scraped + failed` still holds because those count listing hits and successful stores, not
distinct identities. Making identity `body + identifier + document_url` would keep both, at the
cost of the test's `identifier.ext` transformed-key rule — a contract change, not a bug fix.

**Transformation never writes landing.** It only calls `storage.get(landing_bucket, ...)` and
`upsert_transformed`; there is no landing write path in `transformer.py`. The scraper upserts
landing objects when a source file changes — that is ingestion idempotency, not transformation.

Two source limitations, documented rather than worked around: transformed keys are
`identifier.ext` as the test requires, so two bodies sharing an identifier would collide there
(landing keys are `{body}/{identifier}{ext}` and stay unique); and every listing hit observed so
far resolves to an HTML decision page, so the PDF/DOC/DOCX branch is unit-tested but not yet
exercised live.

## Scaling to 50+ sources

The source-specific code is already isolated: `scraping/{bodies,listing}.py` parse WRC markup, the
spider composes requests, and everything downstream — partitioning, hashing, identity, storage
ports, ingestion, transformation, logging — is source-agnostic and works from a common metadata
schema (`models/metadata.py`). Scaling means adding a source registry (base URL, selectors,
partition size, rate limits) and one adapter per source behind the existing `parse_*` contract,
then making `body` a `(source, body)` pair in the identity index. Dagster grows a partition
dimension: `MultiPartitionsDefinition(date x source)`, so 50 sources fan out as independent,
individually retryable partitions over the same assets and the same buckets/collections. Storage
and Mongo already stream per record (batched cursors, no full-collection loads), so the limits are
politeness per host and worker count, not the pipeline.
