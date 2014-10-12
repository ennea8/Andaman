# encoding=utf-8
__author__ = 'lxf'

import re
# from os import *
import pymongo
from scrapy import Request, Selector, Item, Field
from scrapy.contrib.spiders import CrawlSpider
from utils import get_mongodb
import math


# ----------------------------define field------------------------------------
class citytempItem(Item):
    moeid = Field()  # 地区的moeid which represent area
    country = Field()  # 国家
    city = Field()  # 城市
    currrent_temprature = Field()  # 当前时刻的温度
    future_temprature = Field()  # 未来温度预测


# ---------------------------连接mongo----------------------------------------------
class DBMongo:
    countrylist = set([tmp['code'] for tmp in get_mongodb('geo', 'Country').find({}, {'code': 1})])  # bug,等待数据库的更新,国家代码列表
    citylist = get_mongodb('geo', 'Country').find()  # bug,如何设计结构;城市列表,将州和城市做相同的处理,city format:[{'bejing':(100,200)},{..}]


# ----------------------------------define spider------------------------------------
class tempratureSpider(CrawlSpider):
    name = 'tempraturespider'  # define the spider name

    countrylist = DBMongo.countrylist
    citylist = DBMongo.citylist

    # ---------------------------draw the url-----------------------------------------
    def start_requests(self):  # send request
        #bug how to use the name of city and country
        for country in self.countrylist:
            for i in len(self.citylist):
                url = 'http://search.yahoo.com/sugg/gossip/gossip-gl-location/?appid=weather' \
                      '&output=xml&lc=en-US&command=%s%2C%s' % self.citylist[i].keys()[0] % country
                data = {'country': country, 'city': self.citylist[i]}
                yield Request(url=url, callback=self.parse_url, meta={'data': data})

    # ------------------------draw temprature-------------------------------------
    def parse_url(self, response):  # draw the state
        sel = Selector(response)
        xml_city = sel.xpath('//m/s/@k').extract()  #parse xml to get the city
        xml_info = sel.xpath('//m//s/@d').extract()  #parse xml to get the lat and long
        cityinfo = response.meta['data']['city']  #{'cityname':(citylat,citylon)}
        city_lat = cityinfo.values()[0][0]
        city_lon = cityinfo.values()[0][1]
        cityname = cityinfo.keys()[0]
        item = citytempItem()
        item['country'] = response.meta['data']['country']
        item['city'] = cityname
        min = 10000
        i = 0
        if xml_city:
            for i in len(xml_city):
                if cityname == xml_city[i]:
                    location = dict(
                        [(tmp[0], float(tmp[1])) for tmp in re.findall(r'(lat|lon)=([-?\d\.]+)', xml_info[i])])
                    lat = location['lat']
                    lon = location['lon']
                    distance = math.sqrt((lat - city_lat) ** 2 + (long - city_lon) ** 2)
                    if distance < min:
                        min = distance
                    else:
                        continue
                else:
                    continue
            xml_woeid = re.search(r'\d{1,}', xml_info[i]).group()
            item['woeid'] = xml_woeid
            url = 'http://weather.yahooapis.com/forecastrss?w=%d&u=c' % xml_woeid
            data = {'item': item}
            yield Request(url=url, callback=self.parse, meta={'data': data})
        else:
            return


    # ------------------------draw the third url-------------------------------------
    def parse(self, response):
        sel = Selector(response)
        xml_current_temprature = sel.xpath('//item/yweather:condition/@*').extract()    #maybe a bug
        xml_future_temprature = sel.xpath('//item/yweather:forecast/@*').extract()
        item = response.meta['data']['item']
        item['current_temprature'] = xml_current_temprature
        item['future_temprature'] = xml_future_temprature
        return item


    #-----------------------pipeline--------------------------------------------------

class city_name_itemPipeline(object):
    def process_item(self, item, spider):
        data = {}
        if 'country' in item:
            data['country'] = item['country']
        if 'city' in item:
            data['city'] = item['city']
        if 'moeid' in item:
            data['moeid'] = item['moeid']
        if 'current_temprature' in item:
            data['current_temprature'] = item['current_temprature']
        if 'future_temprature' in item:
            data['future_temprature'] = item['future_temprature']
        col = pymongo.MongoClient().raw_data.temprature
        col.save(data)
        return item

