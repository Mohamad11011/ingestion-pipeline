from __future__ import annotations

from pydantic import BaseModel


class LandingMetadata(BaseModel):
    identifier: str
    title: str
    description: str
    date: str
    document_url: str
    partition_date: str
    body: str
    file_path: str
    file_hash: str
    source_url: str | None = None
    document_type: str | None = None
    scraped_at: str | None = None
    http_etag: str | None = None
    http_last_modified: str | None = None

    @property
    def identity(self) -> tuple[str, str]:
        return (self.body, self.identifier)
