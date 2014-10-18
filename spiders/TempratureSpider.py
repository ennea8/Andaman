# encoding=utf-8

__author__ = 'lxf'

# from os import *
from scrapy import Request, Selector, Item, Field
from scrapy.contrib.spiders import CrawlSpider
from utils import get_mongodb
import time


# ----------------------------define field------------------------------------
class CityTempratureItem(Item):
    loc = Field()  # location
    current = Field()
    forecast = Field()
    updateDate = Field()


# ----------------------------------define spider------------------------------------
class CityTempratureSpider(CrawlSpider):
    name = 'citytempraturespider'  # define the spider name

    # def time_format1(temp_time):
    # format_time = datetime.datetime.strptime(temp_time, '%a, %d %d %Y %H:%M %p %Z')
    # gettime = format_time.strftime('%Y %m %d %a %H %M')
    # return gettime
    #
    # def time_format2(temp_time):
    #     format_time = datetime.datetime.strptime(temp_time, '%a %d %m %Y')
    #     gettime = format_time.strftime('%Y %m %d %a')
    #     return gettime

    # ---------------------------draw the info-----------------------------------------

    def start_requests(self):  # send request
        col = get_mongodb('raw_data', 'CityInfo', profile='mongodb-crawler')  # get the collection of cityinfo
        for temp in col.find({}, {'city': 1, 'woeid': 1}):
            city = temp['city']
            city_id = temp['_id']
            woeid = temp['woeid']
            url = 'http://weather.yahooapis.com/forecastrss?w=%d&u=c' % woeid
            data = {'city_id': city_id, 'city': city}
            yield Request(url=url, callback=self.parse, meta={'data': data})

    # ------------------------draw temprature-------------------------------------

    def parse(self, response):
        sel = Selector(response)
        item = CityTempratureItem()
        current = sel.xpath('//item/*[name()="yweather:condition"]/@*').extract()  # maybe a bug
        forecast = sel.xpath('//item/*[name()="yweather:forecast"]/@*').extract()
        data = response.meta['data']
        item['loc'] = {'enName': data['city'], 'city_id': data['city_id']}

        if current:
            current_temp = {
                #'time': self.time_format1(current[3]),
                'currTemprature': float(current[2]),
                'desc': current[0],
                'code': int(current[1])
            }
        else:
            current_temp = None
        item['current'] = current_temp
        if forecast:
            forecast_temp = [
                {
                    #'time': self.time_format2(forecast[6] + ' ' + forecast[7]),
                    'lowerTemprature': float(forecast[8]),
                    'upperTemprature': float(forecast[9]),
                    'desc': forecast[10],
                    'code': int(forecast[5])
                },
                {
                    #'time': self.time_format2(forecast[12] + ' ' + forecast[13]),
                    'lowerTemprature': float(forecast[14]),
                    'upperTemprature': float(forecast[15]),
                    'desc': forecast[16],
                    'code': int(forecast[11])
                },
                {
                    #'time': self.time_format2(forecast[18] + ' ' + forecast[19]),
                    'lowerTemprature': float(forecast[20]),
                    'upperTemprature': float(forecast[21]),
                    'desc': forecast[22],
                    'code': int(forecast[17])
                },
                {
                    #'time': self.time_format2(forecast[24] + ' ' + forecast[25]),
                    'lowerTemprature': float(forecast[26]),
                    'upperTemprature': float(forecast[27]),
                    'desc': forecast[28],
                    'code': int(forecast[23])
                }
            ]
        else:
            forecast_temp = None
        item['forecast'] = forecast_temp
        item['updateDate'] = time.strftime('%Y/%m/%d %H:%M')
        yield item


        # -----------------------pipeline--------------------------------------------------


class CityTempraturePipeline(object):
    spiders = [CityTempratureSpider.name]  # 注册spider

    def process_item(self, item, spider):
        data = {}
        if 'loc' in item:
            data['loc'] = item['loc']
        if 'current' in item:
            data['current'] = item['current']
        if 'forecast' in item:
            data['forecast'] = item['forecast']
        if 'current' in item:
            data['current'] = item['current']
        if 'updateDate' in item:
            data['updateDate'] = item['updateDate']
        col = get_mongodb('yahooweather', 'CityTemprature', profile=None)
        col.save(data)
        return item

