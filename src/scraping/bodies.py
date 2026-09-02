from __future__ import annotations

from dataclasses import dataclass

from parsel import Selector

_BODY_GROUP = "#CB2"


@dataclass(frozen=True)
class Body:
    value: str
    name: str


def parse_bodies(selector: Selector) -> list[Body]:
    """Bodies come from the site's own filter checkboxes; never from a hardcoded list."""
    bodies: list[Body] = []
    for checkbox in selector.css(f"{_BODY_GROUP} input[type=checkbox]"):
        value = (checkbox.attrib.get("value") or "").strip()
        checkbox_id = checkbox.attrib.get("id") or ""
        name = (selector.css(f'{_BODY_GROUP} label[for="{checkbox_id}"]::text').get() or "").strip()
        if value and name:
            bodies.append(Body(value=value, name=name))
    return bodies
