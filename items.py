# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class WeatherItem(scrapy.Item):
    # define the fields for your item here like:
    id = scrapy.Field()
    province = scrapy.Field()
    city = scrapy.Field()
    county = scrapy.Field()
    data = scrapy.Field()


class QiniuyunItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pic = scrapy.Field()
    url = scrapy.Field()
    key = scrapy.Field()
    hash_value = scrapy.Field()


class TravelNotesItem(scrapy.Item):
    user_name = scrapy.Field()
    user_url = scrapy.Field()

    start_year = scrapy.Field()
    start_month = scrapy.Field()
    origin = scrapy.Field()
    destination = scrapy.Field()
    time = scrapy.Field()
    cost = scrapy.Field()

    quality = scrapy.Field()

    title = scrapy.Field()
    content = scrapy.Field()
    reply = scrapy.Field()
    view = scrapy.Field()
    recommend = scrapy.Field()
    favourite = scrapy.Field()