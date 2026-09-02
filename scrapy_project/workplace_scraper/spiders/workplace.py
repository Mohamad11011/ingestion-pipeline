from __future__ import annotations

import scrapy


class WorkplaceSpider(scrapy.Spider):
    """WRC decisions spider. Implemented in Phase 3 from the Phase 0 contract."""

    name = "workplace"
    allowed_domains = ["workplacerelations.ie"]

    def start_requests(self):
        return []
