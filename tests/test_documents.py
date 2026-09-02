from __future__ import annotations

import pytest

from models.documents import (
    DOC,
    DOCX,
    HTML,
    PDF,
    detect_document_type,
    landing_key,
    sanitize_identifier,
    transformed_key,
)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("ADJ-00039955", "ADJ-00039955"),
        ("  LCR22916  ", "LCR22916"),
        ("ADJ/00012345", "ADJ-00012345"),
        ("PWD 246", "PWD_246"),
    ],
)
def test_sanitize_keeps_identifier_readable(raw: str, expected: str) -> None:
    assert sanitize_identifier(raw) == expected


def test_sanitize_rejects_empty_identifier() -> None:
    with pytest.raises(ValueError):
        sanitize_identifier("///")


def test_signature_beats_content_type() -> None:
    assert detect_document_type("text/html", b"%PDF-1.7 ...") == PDF
    assert detect_document_type(None, b"PK\x03\x04rest") == DOCX
    assert detect_document_type(None, b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1rest") == DOC


def test_content_type_used_without_signature() -> None:
    assert detect_document_type("application/pdf; charset=binary", b"") == PDF
    assert detect_document_type("text/html", b"<html></html>") == HTML


def test_landing_key_is_namespaced_by_body() -> None:
    assert (
        landing_key("Labour Court", "LCR22916", HTML) == "Labour_Court/LCR22916.html"
    )


def test_transformed_key_is_identifier_dot_ext() -> None:
    assert transformed_key("ADJ-00039955", PDF) == "ADJ-00039955.pdf"
    assert transformed_key("ADJ-00039955", HTML) == "ADJ-00039955.html"
