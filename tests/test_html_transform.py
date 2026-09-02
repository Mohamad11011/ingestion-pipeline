from __future__ import annotations

from pathlib import Path

import pytest
from bs4 import BeautifulSoup

from transformation.html import ContentNotFound, clean_html

FIXTURE = Path(__file__).parent / "fixtures" / "sample_decision.html"

# Site UI present in the real fixture and expected to disappear.
UI_TEXT = [
    "Return to Search",
    "I accept cookies from this site",
    "Cookie Management",
    "Data Protection",
    "Sitemap",
    "Gaeilge",
    "Skip to main content",
]

# Real decision content that must survive.
DECISION_TEXT = [
    "ADJUDICATION OFFICER RECOMMENDATION",
    "ADJ-00039955",
    "Section 13 of the Industrial Relations Acts",
    "Background:",
    "Findings and Conclusions:",
    "Recommendation:",
    "Louise Boyle",
    "Redeployment",
]


@pytest.fixture(scope="module")
def cleaned() -> str:
    return clean_html(FIXTURE.read_bytes()).decode("utf-8")


def test_site_ui_is_removed(cleaned: str) -> None:
    for fragment in UI_TEXT:
        assert fragment not in cleaned


def test_structural_ui_tags_are_removed(cleaned: str) -> None:
    soup = BeautifulSoup(cleaned, "lxml")
    for tag in ("nav", "footer", "button", "script", "style", "form", "iframe", "input"):
        assert soup.find(tag) is None, tag
    assert soup.select("a.btn, .return-to-search, .cookie, .social-banner") == []


def test_decision_content_is_preserved(cleaned: str) -> None:
    for fragment in DECISION_TEXT:
        assert fragment in cleaned


def test_output_is_a_standalone_html_document(cleaned: str) -> None:
    assert cleaned.startswith("<!DOCTYPE html>")
    soup = BeautifulSoup(cleaned, "lxml")
    assert soup.title is not None
    assert soup.body is not None


def test_cleaning_is_deterministic() -> None:
    payload = FIXTURE.read_bytes()
    assert clean_html(payload) == clean_html(payload)


def test_missing_content_container_raises() -> None:
    with pytest.raises(ContentNotFound):
        clean_html(b"<html><body><nav>menu</nav></body></html>")
