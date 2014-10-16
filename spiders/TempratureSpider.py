# encoding=utf-8
import utils

__author__ = 'lxf'

import re
# from os import *
from scrapy import Request, Selector, Item, Field
from scrapy.contrib.spiders import CrawlSpider
from utils import get_mongodb
import math


# ----------------------------define field------------------------------------
class CityTempratureItem(Item):
    loc = Field()  # location
    current = Field()
    forecast = Field()
    source = Field()


# ----------------------------------define spider------------------------------------
class CityTempratureSpider(CrawlSpider):
    name = 'citytempraturespider'  # define the spider name

    # ---------------------------draw the info-----------------------------------------

    def start_requests(self):  # send request
        col = get_mongodb('raw_data', 'CityInfo', profile='mongodb-crawler')  # get the collection of cityinfo
        for temp in self.col.find({}, {'city': 1, 'woeid': 1}):
            city = temp['city']
            woeid = temp['woeid']
            _id=temp['_id']
            url = 'http://weather.yahooapis.com/forecastrss?w=%d&u=c' % woeid
            data = {'_id':_id,'city': city, 'woeid': woeid}
            yield Request(url=url, callback=self.parse, meta={'data': data})

    # ------------------------draw temprature-------------------------------------

    def parse(self, response):
        sel = Selector(response)
        item = CityTempratureItem()
        current = sel.xpath('//item/*[name()="yweather:condition"]/@*').extract()  # maybe a bug
        forecast = sel.xpath('//item/*[name()="yweather:forecast"]/@*').extract()
        data = response.meta['data']
        item['loc'] = {'enname': data['city'],'_id':data['_id']}
        item['source'] = {'woeid': data['woeid']}

        if current:
            current_temp = {
                'time': current[3],
                'temprature': current[2],
                'desc': current[0]
            }
        else:
            current_temp = None
        item['current'] = current_temp
        if forecast:
            forecast_temp = [
                {
                    'time': forecast[6] + ' ' + forecast[7],
                    'lowertemprature': forecast[8],
                    'uppertemprature': forecast[9],
                    'desc': forecast[10]
                },
                {
                    'time': forecast[12] + ' ' + forecast[13],
                    'lowertemprature': forecast[14],
                    'uppertemprature': forecast[15],
                    'desc': forecast[16]
                },
                {
                    'time': forecast[18] + ' ' + forecast[19],
                    'lowertemprature': forecast[20],
                    'uppertemprature': forecast[21],
                    'desc': forecast[22]
                },
                {
                    'time': forecast[24] + ' ' + forecast[25],
                    'lowertemprature': forecast[26],
                    'uppertemprature': forecast[27],
                    'desc': forecast[28]
                }
            ]
        else:
            forecast_temp = None
        item['forecast'] = forecast_temp
        return item


        # -----------------------pipeline--------------------------------------------------


class CityTempraturePipeline(object):
    spiders = [CityTempratureSpider.name]  # 注册spider

    def process_item(self, item, spider):
        data = {}
        if 'loc' in item:
            data['loc'] = item['loc']
        if 'current_temprature' in item:
            data['current_temprature'] = item['current_temprature']
        if 'forecast' in item:
            data['forecast'] = item['forecast']
        if 'current' in item:
            data['current'] = item['current']
        if 'source' in item:
            data['source'] = item['source']
        col = get_mongodb('raw_data', 'CityTemprature', profile='mongo-crawler')
        col.save(data)
        return item

