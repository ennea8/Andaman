# coding=utf-8
import scrapy


class GitHubItem(scrapy.Item):
    item_type = scrapy.Field()
    data = scrapy.Field()
