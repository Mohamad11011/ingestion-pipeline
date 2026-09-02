from __future__ import annotations

import re

HTML = "html"
PDF = "pdf"
DOC = "doc"
DOCX = "docx"

_EXTENSIONS = {HTML: ".html", PDF: ".pdf", DOC: ".doc", DOCX: ".docx"}
_ILLEGAL_KEY_CHARS = re.compile(r"[\/\x00-\x1f]+")
_COLLAPSE = re.compile(r"\s+")


def sanitize_identifier(identifier: str) -> str:
    """Strip only characters illegal in an object key; keep the identifier readable."""
    cleaned = _ILLEGAL_KEY_CHARS.sub("-", identifier.strip())
    cleaned = _COLLAPSE.sub("_", cleaned).strip("-_.")
    if not cleaned:
        raise ValueError(f"identifier is empty after sanitization: {identifier!r}")
    return cleaned


def extension_for(document_type: str) -> str:
    try:
        return _EXTENSIONS[document_type]
    except KeyError:
        raise ValueError(f"unsupported document type: {document_type!r}") from None


def detect_document_type(content_type: str | None, payload: bytes) -> str:
    """File signature wins over Content-Type, which the site sets loosely."""
    if payload.startswith(b"%PDF-"):
        return PDF
    if payload.startswith(b"PK\x03\x04"):
        return DOCX
    if payload.startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"):
        return DOC
    normalized = (content_type or "").split(";")[0].strip().lower()
    if normalized == "application/pdf":
        return PDF
    if normalized == "application/msword":
        return DOC
    if normalized == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
        return DOCX
    return HTML


def landing_key(body: str, identifier: str, document_type: str) -> str:
    return f"{sanitize_identifier(body)}/{sanitize_identifier(identifier)}{extension_for(document_type)}"


def transformed_key(identifier: str, document_type: str) -> str:
    return f"{sanitize_identifier(identifier)}{extension_for(document_type)}"
