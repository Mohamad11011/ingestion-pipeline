from scrapy import Field, Item


class WorkplaceItem(Item):
    """Carries one scraped record into the landing pipeline; maps into LandingMetadata."""

    record = Field()
    payload = Field()
    content_type = Field()
    existing = Field()
    scope_key = Field()
