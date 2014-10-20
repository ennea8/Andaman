# encoding=utf-8

__author__ = 'lxf'

# from os import *
from scrapy import Request, Selector, Item, Field
from scrapy.contrib.spiders import CrawlSpider
from utils import get_mongodb
import datetime


# ----------------------------define field------------------------------------
class CityTemperatureItem(Item):
    loc = Field()  # location
    current = Field()
    forecast = Field()
    updateTime = Field()


# ----------------------------------define spider------------------------------------
class CityTemperatureSpider(CrawlSpider):
    name = 'citytemperaturespider'  # define the spider name

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
        item = CityTemperatureItem()
        current = sel.xpath('//item/*[name()="yweather:condition"]/@*').extract()  # maybe a bug
        forecast = sel.xpath('//item/*[name()="yweather:forecast"]/@*').extract()
        data = response.meta['data']
        item['loc'] = {'enName': data['city'], 'city_id': data['city_id']}

        if current:
            current_temp = {
                #'time': self.time_format1(current[3]),
                'currTemperature': float(current[2]),
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
                    'lowerTempreature': float(forecast[8]),
                    'upperTempreature': float(forecast[9]),
                    'desc': forecast[10],
                    'code': int(forecast[5])
                },
                {
                    #'time': self.time_format2(forecast[12] + ' ' + forecast[13]),
                    'lowerTemperature': float(forecast[14]),
                    'upperTemperature': float(forecast[15]),
                    'desc': forecast[16],
                    'code': int(forecast[11])
                },
                {
                    #'time': self.time_format2(forecast[18] + ' ' + forecast[19]),
                    'lowerTemperature': float(forecast[20]),
                    'upperTemperature': float(forecast[21]),
                    'desc': forecast[22],
                    'code': int(forecast[17])
                },
                {
                    #'time': self.time_format2(forecast[24] + ' ' + forecast[25]),
                    'lowerTemperature': float(forecast[26]),
                    'upperTemperature': float(forecast[27]),
                    'desc': forecast[28],
                    'code': int(forecast[23])
                }
            ]
        else:
            forecast_temp = None
        item['forecast'] = forecast_temp
        item['updateTime'] = datetime.datetime.now()
        yield item


        # -----------------------pipeline--------------------------------------------------

class CityTemperaturePipeline(object):
    spiders = [CityTemperatureSpider.name]  # 注册spider

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
        if 'updateTime' in item:
            data['updateTime'] = item['updateTime']
        col = get_mongodb('yahooweather', 'CityTemperature', profile=None)
        col.save(data)
        return item

