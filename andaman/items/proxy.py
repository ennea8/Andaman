import scrapy

__author__ = 'zephyre'


class ProxyItem(scrapy.Item):
    host = scrapy.Field()
    port = scrapy.Field()
    scheme = scrapy.Field()
    desc = scrapy.Field()
    latency = scrapy.Field()
    verifiedTime = scrapy.Field()
