# encoding=utf-8
__author__ = 'lxf'

import re
from os import *
import pymongo
from scrapy import Request, Selector, Item, Field
from scrapy.contrib.spiders import CrawlSpider
from utils import get_mongodb

# -----------------------------------define field------------------------
class city_name_item(Item):
    country = Field()  # 国家名称
    state = Field()  # 州/省份
    city = Field()  # 城市


# ---------------------------连接mongo-----------------------------------------------
class DBMongo:
    countrylist = ['America']
    #countrylist = set([tmp['code'] for tmp in get_mongodb('geo', 'Country').find({}, {'code': 1})])


# ----------------------------------define spider------------------------------------
class citynameSpider(CrawlSpider):
    name = 'citynamespider'  # define the spider name

    country_list = DBMongo.countrylist  # 获取国家代码

    # ---------------------------draw the url-----------------------------------------
    def start_requests(self):  # send request
        first_url = 'https://weather.yahoo.com/'
        item = city_name_item()
        for countryname in self.country_list:
            temp_url = first_url + countryname
            item['country'] = countryname
            data = {'item': item, 'url': temp_url}
            yield Request(url=temp_url, callback=self.parse_state_url, meta={'data': data})

    # ------------------------draw the state url-------------------------------------
    def parse_state_url(self, response):  # draw the state
        sel = Selector(response)
        state_list = sel.xpath('//div[@id="page1"]/ul/li/a/span/text()').extract()  # state list
        url_state_list = sel.xpath('//div[@id="page1"]/ul/li/a/@href').extract()  # url for state_list
        item = response.meta['data']['item']
        country_url = response.meta['data']['url']
        data = {'item': item}
        if state_list:
            item['state'] = state_list
            for tmp_url in url_state_list:
                url = country_url + tmp_url
                yield Request(url=url, callback=self.parse_city, meta={'data': data})
        else:
            return

    # ------------------------draw the city url-------------------------------------
    def parse_city(self, response):
        item = response.meta['data']['item']
        sel = Selector(self)
        city = sel.xpath('//div[@id="page1"]/ul/li/a/span/text()').extract()  # city list
        if city:
            item['city'] = city
        return item

    # -----------------------pipeline--------------------------------------------------

class city_name_itemPipeline(object):
    def process_item(self, item, spider):
        data = {}
        if 'country' in item:
            data['country'] = item['country']
        if 'state' in item:
            data['state'] = item['city']
        if 'city' in item:
            data['city'] = item['spot']
        col = pymongo.MongoClient().raw_data.cityname
        col.save(data)
        return item

