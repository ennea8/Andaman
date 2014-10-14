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

     #countrylist = ['America']
     countrylist = set([tmp['code'] for tmp in get_mongodb('geo', 'Country').find({}, {'code': 1})])

# ----------------------------------define spider------------------------------------
class citynameSpider(CrawlSpider):
    name = 'citynamespider'  # define the spider name

    country_list = DBMongo.countrylist  # 获取国家代码

    # ---------------------------draw the url-----------------------------------------
    def start_requests(self):  # send request
        first_url = 'https://weather.yahoo.com/'
        for countryname in self.country_list:
            temp_url = first_url + countryname
            data = {'url': temp_url, 'countryname': countryname}
            yield Request(url=temp_url, callback=self.parse_state_url, meta={'data': data})

    # ------------------------draw the state url-------------------------------------
    def parse_state_url(self, response):  # draw the state
        sel = Selector(response)
        state_list = sel.xpath('//div[@id="page1"]/ul/li/a/span/text()').extract()  # state list
        url_state_list = sel.xpath('//div[@id="page1"]/ul/li/a/@href').extract()  # url for state_list
        country_url = response.meta['data']['url']
        countryname = response.meta['data']['countryname']                      #the country to be used next
        if state_list:
            #item['state'] = state_list
            for i in range(0,len(state_list)):
                state_url = country_url + url_state_list[i]
                data = {'countryname': countryname, 'state': state_list[i]}
                yield Request(url=state_url, callback=self.parse_city, meta={'data': data})
        else:
            return

    # ------------------------draw the city url-------------------------------------
    def parse_city(self, response):
        sel = Selector(response)
        city = sel.xpath('//div[@id="page1"]/ul/li/a/span/text()').extract()  # city list
        data = response.meta['data']
        item = city_name_item()
        item['country'] = data['countryname']
        item['state'] = data['state']
        if city:
            item['city'] = city
        return item

        # -----------------------pipeline--------------------------------------------------


class city_name_itemPipeline(object):
    # 向pipline注册
    spiders = [citynameSpider.name]

    def process_item(self, item, spider):
        data = {}
        if 'country' in item:
            data['country'] = item['country']
        if 'state' in item:
            data['state'] = item['state']
        if 'city' in item:
            data['city'] = item['city']
        col = get_mongodb('raw_data', 'CityName', profile='mongo-crawler')
        col.save(data)
        return item

