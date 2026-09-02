from scrapy import Field, Item


class WorkplaceItem(Item):
    identifier = Field()
    title = Field()
    description = Field()
    date = Field()
    document_url = Field()
    partition_date = Field()
    body = Field()
    source_url = Field()
    document_type = Field()
