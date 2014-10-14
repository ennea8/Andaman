# encoding=utf-8
import utils

__author__ = 'lxf'

import re
# from os import *
import pymongo
from scrapy import Request, Selector, Item, Field
from scrapy.contrib.spiders import CrawlSpider
from utils import get_mongodb
import math


# ----------------------------define field------------------------------------
class citytempratureItem(Item):
    country_code = Field()  # 国家
    city = Field()  # 城市
    moeid = Field()  # 地区的moeid which represent area
    currrent_temprature = Field()  # 当前时刻的温度
    future_temprature = Field()  # 未来温度预测


# ----------------------------------define spider------------------------------------
class tempratureSpider(CrawlSpider):
    name = 'tempraturespider'  # define the spider name

    # ---------------------------draw the url-----------------------------------------
    def start_requests(self):  # send request

        for tmp1 in get_mongodb('geo', 'Country').find({}, {'code': 1}):
            country_code = tmp1['code']
            #country_code='cn'
            for temp2 in get_mongodb('geo', 'Locality').find({'countryId': country_code.upper()},{'coords': 1, 'zhName': 1}):
                cityname = temp2['zhName']
                coords = temp2['coords']
                url = 'http://search.yahoo.com/sugg/gossip/gossip-gl-location/?appid=weather&output=xml&lc=en-US&command=%s,%s' % cityname % country_code
                data = {'country_code': country_code, 'city': cityname, 'coords': coords}
                yield Request(url=url, callback=self.parse_url, meta={'data': data})

    # ------------------------draw temprature-------------------------------------
    def parse_url(self, response):  # draw the state
        sel = Selector(response)
        xml_city = sel.xpath('//m/s/@k').extract()  # parse xml to get the city
        xml_info = sel.xpath('//m//s/@d').extract()  # parse xml to get the lat and lng
        data1 = response.meta['data']
        cityname = data1['city']
        coords = data1['coords']
        city_lat = coords['lat']
        city_lng = coords['lng']
        min = 1000000000
        i = 0
        #get the woeid
        if xml_city:
            for i in len(xml_city):
                if cityname == xml_city[i]:
                    location = dict(
                        [(tmp[0], float(tmp[1])) for tmp in re.findall(r'(lat|lon)=([-?\d\.]+)', xml_info[i])])
                    lat = location['lat']
                    lon = location['lon']
                    distance = math.sqrt((lat - city_lat) ** 2 + (long - city_lng) ** 2)
                    if distance < min:
                        min = distance
                    else:
                        continue
                else:
                    continue
            woeid = re.search(r'\d{1,}', xml_info[i]).group()
            url = 'http://weather.yahooapis.com/forecastrss?w=%d&u=c' % woeid
            data = {'data1': data1, 'moeid': woeid}
            yield Request(url=url, callback=self.parse, meta={'data': data})
        else:
            return


    # ------------------------draw the third url-------------------------------------
    def parse(self, response):
        sel = Selector(response)
        xml_current_temprature = sel.xpath('//item/yweather:condition/@*').extract()  # maybe a bug
        xml_future_temprature = sel.xpath('//item/yweather:forecast/@*').extract()
        data = response.meta['data']
        item = citytempratureItem()
        item['country_code'] = data['data1']['country_code']
        item['city'] = data['data1']['city']
        item['woeid'] = data['woeid']
        item['current_temprature'] = xml_current_temprature
        item['future_temprature'] = xml_future_temprature
        return item


        # -----------------------pipeline--------------------------------------------------


class city_name_itemPipeline(object):
    spiders = [tempratureSpider.name]  # 注册spider

    def process_item(self, item, spider):
        data = {}
        if 'country_code' in item:
            data['country_code'] = item['country_code']
        if 'city' in item:
            data['city'] = item['city']
        if 'moeid' in item:
            data['moeid'] = item['moeid']
        if 'current_temprature' in item:
            data['current_temprature'] = item['current_temprature']
        if 'future_temprature' in item:
            data['future_temprature'] = item['future_temprature']
        col = get_mongodb('raw_data', 'CityTemprature', profile='mongo-crawler')
        col.save(data)
        return item

