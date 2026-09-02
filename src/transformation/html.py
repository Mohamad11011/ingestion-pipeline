from __future__ import annotations

from html import escape

from bs4 import BeautifulSoup, Tag

# Proven against tests/fixtures/sample_decision.html (a real WRC decision page):
# the decision body lives in div.content inside div.col-sm-9. The rest are fallbacks
# for older/other layouts on the same site.
CONTENT_SELECTORS = ("div.content", "main", "#content", "div.col-sm-9", "article")

UI_SELECTORS = (
    "script",
    "style",
    "noscript",
    "iframe",
    "nav",
    "header",
    "footer",
    "form",
    "button",
    "input",
    "select",
    "[role=navigation]",
    "[role=banner]",
    "[role=contentinfo]",
    ".cookie",
    ".top-header",
    ".logo-header",
    ".searchbanner",
    ".social-banner",
    ".language-switch",
    ".google-translate",
    ".resize",
    ".return-to-search",
    ".GAButton",
    ".btn",
    ".pager",
    ".sr-only",
    ".skiplink-text",
)

_KEEP_ATTRS = frozenset({"href", "src", "alt", "title", "colspan", "rowspan", "lang"})


class ContentNotFound(Exception):
    """No decision container matched; the document is logged as failed and skipped."""


def clean_html(payload: bytes) -> bytes:
    soup = BeautifulSoup(payload, "lxml")
    container = _find_content(soup)
    _strip_ui(container)
    _strip_attributes(container)
    return _render(_title(soup, container), container).encode("utf-8")


def _find_content(soup: BeautifulSoup) -> Tag:
    for selector in CONTENT_SELECTORS:
        container = soup.select_one(selector)
        if container is not None and container.get_text(strip=True):
            return container
    raise ContentNotFound(f"no container matched {CONTENT_SELECTORS}")


def _strip_ui(container: Tag) -> None:
    for selector in UI_SELECTORS:
        for element in container.select(selector):
            element.decompose()


def _strip_attributes(container: Tag) -> None:
    for element in [container, *container.find_all(True)]:
        element.attrs = {k: v for k, v in element.attrs.items() if k in _KEEP_ATTRS}


def _title(soup: BeautifulSoup, container: Tag) -> str:
    for candidate in (soup.title, container.find(["h1", "h2"])):
        if candidate is not None and candidate.get_text(strip=True):
            return candidate.get_text(strip=True)
    return "decision"


def _render(title: str, container: Tag) -> str:
    return (
        "<!DOCTYPE html>\n"
        '<html lang="en">\n'
        '<head><meta charset="utf-8"><title>'
        f"{escape(title)}"
        "</title></head>\n"
        f"<body>\n{container.decode()}\n</body>\n</html>\n"
    )
