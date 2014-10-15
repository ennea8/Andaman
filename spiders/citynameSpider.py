# encoding=utf-8
__author__ = 'lxf'

import re
from os import *
import pymongo
from scrapy import Request, Selector, Item, Field
from scrapy.contrib.spiders import CrawlSpider
from utils import get_mongodb

# -----------------------------------define field------------------------
class city_item(Item):
    country = Field()  # 国家信息
    state = Field()  # 州/省份
    city = Field()  # 城市
    level = Field()  #
    misc = Field()
    abroad = Field()
    alias = Field()
    coords = Field()
    zhName = Field()
    pinyin = Field()
    images = Field()
    shortName = Field()
    imageList = Field()
    enName = Field()
    woeid = Field()


# ---------------------------连接mongo-----------------------------------------------
class DBMongo:
    countrycodelist = ['usa']
    # countrycodelist = set([tmp['code'] for tmp in get_mongodb('geo', 'Country').find({}, {'code': 1})])


# ----------------------------------define spider------------------------------------
class citynameSpider(CrawlSpider):
    name = 'citynamespider'  # define the spider name

    country_code_list = DBMongo.countrycodelist  # 获取国家代码

    # ---------------------------draw the url-----------------------------------------
    def start_requests(self):  # send request
        first_url = 'https://weather.yahoo.com/'
        for countrycode in self.country_code_list:
            if countrycode is not r'CN' and r'cn':
                abroad = 'true'
            else:
                abroad = 'false'
            temp_url = first_url + countrycode
            data = {'countrycode': countrycode, 'abroad': abroad}
            yield Request(url=temp_url, callback=self.parse_state_url, meta={'data': data})

    # ------------------------draw the state url-------------------------------------
    def parse_state_url(self, response):  # draw the state
        sel = Selector(response)
        tempcountryname = sel.xpath(
            '//div[@id="MediaWeatherRegion"]/div[@class="hd"]/div[@class="yom-bread"]/text()').extract()
        match = re.search(r'\w{1,}\s*\w{1,}', tempcountryname[0])
        if match:
            countryname = match.group()
        else:
            countryname = None
        state_list = sel.xpath('//div[@id="page1"]/ul/li/a/span/text()').extract()  # state list
        url_state_list = sel.xpath('//div[@id="page1"]/ul/li/a/@href').extract()  # url for state_list
        data_1 = response.meta['data']

        if state_list:
            for i in range(0, len(state_list)):
                state_url = 'https://weather.yahoo.com' + url_state_list[i]
                data = {'data_1': data_1, 'countryname': countryname, 'state': state_list[i]}
                yield Request(url=state_url, callback=self.parse_city, meta={'data': data})
        else:
            return

    # ------------------------draw the city url-------------------------------------
    def parse_city(self, response):
        sel = Selector(response)
        city_list = sel.xpath('//div[@id="page1"]/ul/li/a/span/text()').extract()  # city list
        city_list_url = sel.xpath('//div[@id="MediaWeatherRegion"]/div[@class="bd"]/'
                                  'div[@class="weather-regions"]/div/ul/li/a/@href').extract()
        data_2 = response.meta['data']
        if city_list:
            for i in range(0, len(city_list)):
                city_url = 'https://weather.yahoo.com' + city_list_url[i]
                data = {'data_2': data_2, 'cityname': city_list[i], 'city_url': city_url}
                yield Request(url=city_url, callback=self.parse_city_info, meta={'data': data})
        else:
            return

    def parse_city_info(self, response):
        sel = Selector(response)
        city_url = response.meta['data']['city_url']
        match = re.search(r'\d{1,}', city_url)
        if match:
            woeid = match.group()
        else:
            woeid = None
        location = re.findall(r'("lat"|"lon"):(")([\d\.]+)(")', response.body)  # bug
        coords = location
        data_3 = response.meta['data']
        country = {'countrycode': data_3['data_2']['data_1']['countrycode'],
                   'countryname': data_3['data_2']['countryname']}
        item = city_item()
        item['country'] = country
        item['state'] = data_3['data_2']['state']
        item['city'] = data_3['cityname']
        item['abroad'] = data_3['data_2']['data_1']['abroad']
        item['alias'] = []
        item['coords'] = []
        item['woeid'] = woeid
        item['zhName'] = []
        item['images'] = []
        item['shortName'] = []
        item['imageList'] = []
        item['misc'] = []
        item['enName'] = data_3['cityname']
        item['pinyin'] = []
        item['level'] = []

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
        if 'abroad' in item:
            data['abroad'] = item['abroad']
        if 'alias' in item:
            data['alias'] = item['alias']
        if 'coords' in item:
            data['coords'] = item['coords']
        if 'woeid' in item:
            data['woeid'] = item['woeid']
        if 'zhName' in item:
            data['zhName'] = item['zhName']
        if 'images' in item:
            data['images'] = item['images']
        if 'imageList' in item:
            data['imageList'] = item['imageList']
        if 'misc' in item:
            data['misc'] = item['misc']
        if 'enName' in item:
            data['enName'] = item['enName']
        if 'pinyin' in item:
            data['pinyin'] = item['pinyin']
        if 'level' in item:
            data['level'] = item['level']
        if 'shortName' in item:
            data['shortName'] = item['shortName']
        col = get_mongodb('raw_data', 'CityInfo', profile='mongo-crawler')
        col.save(data)
        return item