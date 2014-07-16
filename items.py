# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.item import Item, Field


class WeatherItem(scrapy.Item):
    # define the fields for your item here like:
    id = scrapy.Field()
    province = scrapy.Field()
    city = scrapy.Field()
    county = scrapy.Field()
    data = scrapy.Field()
class BlogItem(Item):
    # define the fields for your item here like:
    # name = Field()
    author_url=Field()
    author_name=Field()
    hot=Field()
    tag=Field()
    desc=Field()
    keyword=Field()
    img=Field()