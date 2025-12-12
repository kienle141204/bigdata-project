import scrapy


class RawJobItem(scrapy.Item):
    """Raw job data item"""
    url = scrapy.Field()
    source = scrapy.Field()
    html = scrapy.Field()  # Raw HTML content
    crawled_at = scrapy.Field()
    status = scrapy.Field()