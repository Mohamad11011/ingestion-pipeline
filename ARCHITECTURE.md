# Architecture

```
workplacerelations.ie → Scrapy → Mongo landing.metadata + MinIO landing/
                                      |  (read only)
                                      v
                               Transformation → transformed.metadata + MinIO transformed/
```

Dagster assets: `landing_documents` → `transformed_documents` (transform depends on scrape).

## Date partition size

**Monthly** (`PARTITION_SIZE`; `weekly` is also supported). A month of one body is a handful of listing pages and tens of documents: cheap to retry, readable in the JSON summary, and low overhead at the 500–1000 document scale. Weekly is for dense back-fills.

Partitions are half-open `[start, end)`. `end_date` is exclusive. `partition_date` is the partition start (`YYYY-MM-DD`, Europe/Dublin, date only). The site’s `from`/`to` are inclusive, so the spider sends `to = partition_end − 1 day`. Dagster’s monthly window is the same half-open interval, so one asset partition is one scraper partition.

## Retries and rate limiting

`CONCURRENT_REQUESTS=8` (per domain), `DOWNLOAD_DELAY=1.0s` (randomized), `AUTOTHROTTLE_ENABLED`, `DOWNLOAD_TIMEOUT=30`, identifying `User-Agent`. `RETRY_TIMES=3` with exponential backoff on 408/429/500/502/503/504/522/524. A middleware logs 429/503 and `Retry-After`; AutoThrottle and RetryMiddleware wait. `/en/Cases/` is disallowed by `robots.txt` but is the path the test requires, so `ROBOTSTXT_OBEY=False` with polite rates; bulk-import trees are never requested. Failures are per record: errback logs `url`, `error_code`, `reason` and the run continues (`records_found == records_scraped + failed`).

## Deduplication

Identity is `body + identifier` (unique index `uniq_body_identifier` on both collections; upsert filter). Before fetch, stored `ETag` / `Last-Modified` are sent as conditional headers; `304` skips the download. After download, SHA-256 of the stored bytes is compared to `file_hash`: equal → no put; different → put landing object and upsert metadata.

This source sends **no ETag/Last-Modified**, so the hash is the real guard (`Content-Length` is off by one and unused). Two volatile HTML comments (`Elapsed time`, cache-state) are normalized **before** store/hash so a re-run does not rewrite every page. **Transformation never writes landing** (`get` + `upsert_transformed` only). The scraper upserts landing when source bytes change — that is ingestion, not transform.

Landing keys `{body}/{identifier}{ext}` stay unique. Transformed keys are `identifier.ext` as required, so two bodies sharing an identifier would collide there. The source also repeats two identifiers (`RPD241`, `ADJ-00044064`) on different URLs; identity collapses them (616 listings → 614 records). PDF/DOC/DOCX copy-through is implemented; this site served HTML decision pages in the live range.

## Scaling to 50+ sources

```
Source config → generic framework → adapter A/B/C → common metadata schema → shared storage
```

WRC markup lives in `scraping/{bodies,listing}.py`. Partitioning, hashing, identity, storage ports, ingest, transform, and logging are source-agnostic (`models/metadata.py`). Next: a source registry (URL, selectors, rates) and one adapter each; identity becomes `(source, body, identifier)`. Dagster: `MultiPartitionsDefinition(date × source)` over the same assets and buckets. Mongo/S3 already stream per record, so the limit is politeness per host, not memory.
