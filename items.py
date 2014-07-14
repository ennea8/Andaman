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
	# 123
