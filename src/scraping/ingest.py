from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime

from hashing.files import sha256_bytes
from models.documents import DOC, DOCX, HTML, PDF, detect_document_type, landing_key
from models.metadata import LandingMetadata
from storage.ports import MetadataStore, ObjectStorage

# The site stamps per-request render telemetry into every page: an elapsed-time comment, plus a
# cache-state comment that is present or absent depending on whether the server served the page
# from its own cache. Without normalizing both, the bytes (and so the hash) differ between fetches
# and no document would ever look unchanged.
_RENDER_COMMENTS = (
    (re.compile(rb"<!--\s*Elapsed time:[^>]*-->"), b"<!-- Elapsed time -->"),
    (re.compile(rb"<!--\s*cached or not being index\.aspx page\s*-->"), b""),
)

_CONTENT_TYPES = {
    HTML: "text/html; charset=utf-8",
    PDF: "application/pdf",
    DOC: "application/msword",
    DOCX: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
}


@dataclass(frozen=True)
class IngestResult:
    file_path: str
    file_hash: str
    document_type: str
    inserted: bool
    object_written: bool


def conditional_headers(existing: dict | None) -> dict[str, str]:
    """Section 8 step 2: re-use stored validators so an unchanged file is not downloaded."""
    if not existing:
        return {}
    headers = {}
    if existing.get("http_etag"):
        headers["If-None-Match"] = existing["http_etag"]
    if existing.get("http_last_modified"):
        headers["If-Modified-Since"] = existing["http_last_modified"]
    return headers


def normalize_payload(payload: bytes, document_type: str) -> bytes:
    """Drop volatile server render telemetry so an unchanged page hashes the same twice."""
    if document_type != HTML:
        return payload
    for pattern, replacement in _RENDER_COMMENTS:
        payload = pattern.sub(replacement, payload)
    return payload


class LandingIngestor:
    """Hash-compare, landing object put and landing metadata upsert for one record."""

    def __init__(self, repository: MetadataStore, storage: ObjectStorage, bucket: str) -> None:
        self._repository = repository
        self._storage = storage
        self._bucket = bucket

    def existing(self, body: str, identifier: str) -> dict | None:
        return self._repository.find_landing(body, identifier)

    def ingest(
        self,
        record: dict,
        payload: bytes,
        content_type: str | None,
        existing: dict | None = None,
    ) -> IngestResult:
        document_type = detect_document_type(content_type, payload)
        payload = normalize_payload(payload, document_type)
        file_hash = sha256_bytes(payload)
        file_path = landing_key(record["body"], record["identifier"], document_type)
        # Re-read at write time. The spider's download-time snapshot is stale when two
        # listings share body+identifier (ADJ-00044064 / RPD241) and the other URL already wrote.
        current = self._repository.find_landing(record["body"], record["identifier"]) or existing

        unchanged = (
            current is not None
            and current.get("file_hash") == file_hash
            and current.get("file_path") == file_path
        )
        if not unchanged:
            self._storage.put(self._bucket, file_path, payload, _CONTENT_TYPES[document_type])

        metadata = LandingMetadata(
            **record,
            document_type=document_type,
            file_path=file_path,
            file_hash=file_hash,
            scraped_at=datetime.now(UTC).isoformat(),
        )
        inserted = self._repository.upsert_landing(metadata.model_dump())
        return IngestResult(
            file_path=file_path,
            file_hash=file_hash,
            document_type=document_type,
            inserted=inserted,
            object_written=not unchanged,
        )
