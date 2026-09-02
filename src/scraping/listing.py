from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime

from parsel import Selector

_TOTAL_RESULTS = re.compile(r"of\s+([\d,]+)\s+results", re.IGNORECASE)
_WHITESPACE = re.compile(r"\s+")


@dataclass(frozen=True)
class ListingCard:
    identifier: str
    title: str
    description: str
    date: str
    url: str


def parse_cards(selector: Selector) -> list[ListingCard]:
    return [_card(node) for node in selector.css("li.each-item")]


def parse_total_results(selector: Selector) -> int | None:
    match = _TOTAL_RESULTS.search(" ".join(selector.css(".searchhead::text").getall()))
    if not match:
        match = _TOTAL_RESULTS.search(selector.get() or "")
    return int(match.group(1).replace(",", "")) if match else None


def next_page_href(selector: Selector) -> str | None:
    return selector.css("ul.pager a.next::attr(href)").get()


def _card(node: Selector) -> ListingCard:
    identifier = _clean(node.css("span.refNO::text").get()) or _clean(
        node.css("h2.title a::text").get()
    )
    return ListingCard(
        identifier=identifier,
        title=_clean(node.css("h2.title a::text").get()),
        description=_clean(" ".join(node.css("p.description::text").getall())),
        date=_iso_date(_clean(node.css("span.date::text").get())),
        url=(node.css("h2.title a::attr(href)").get() or "").strip(),
    )


def _clean(value: str | None) -> str:
    return _WHITESPACE.sub(" ", (value or "")).strip()


def _iso_date(value: str) -> str:
    try:
        return datetime.strptime(value, "%d/%m/%Y").date().isoformat()
    except ValueError:
        return value
