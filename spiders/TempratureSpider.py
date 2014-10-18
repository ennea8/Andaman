# encoding=utf-8

__author__ = 'lxf'

# from os import *
from scrapy import Request, Selector, Item, Field
from scrapy.contrib.spiders import CrawlSpider
from utils import get_mongodb
# import datetime, time


# ----------------------------define field------------------------------------
class CityTempratureItem(Item):
    loc = Field()  # location
    current = Field()
    forecast = Field()
    source = Field()


# ----------------------------------define spider------------------------------------
class CityTempratureSpider(CrawlSpider):
    name = 'citytempraturespider'  # define the spider name

    # def time_format1(temp_time):
    # format_time = datetime.datetime.strptime(temp_time, '%a, %d %d %Y %H:%M %p %Z')
    #     gettime = format_time.strftime('%Y %m %d %a %H %M')
    #     return gettime
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
            woeid = temp['woeid']
            city_id = temp['_id']
            url = 'http://weather.yahooapis.com/forecastrss?w=%d&u=c' % woeid
            data = {'city_id': city_id, 'city': city, 'woeid': woeid}
            yield Request(url=url, callback=self.parse, meta={'data': data})

    # ------------------------draw temprature-------------------------------------

    def parse(self, response):
        sel = Selector(response)
        item = CityTempratureItem()
        current = sel.xpath('//item/*[name()="yweather:condition"]/@*').extract()  # maybe a bug
        forecast = sel.xpath('//item/*[name()="yweather:forecast"]/@*').extract()
        data = response.meta['data']
        item['loc'] = {'enname': data['city'], 'city_id': data['city_id']}
        item['source'] = {'name': 'yahoo', 'id': data['woeid']}

        if current:
            current_temp = {
                #'time': self.time_format1(current[3]),
                'temprature': float(current[2]),
                'desc': current[0],
                'desc_code': float(current[1])
            }
        else:
            current_temp = None
        item['current'] = current_temp
        if forecast:
            forecast_temp = [
                {
                    #'time': self.time_format2(forecast[6] + ' ' + forecast[7]),
                    'lowertemprature': float(forecast[8]),
                    'uppertemprature': float(forecast[9]),
                    'desc': forecast[10],
                    'desc_code': int(forecast[5])
                },
                {
                    #'time': self.time_format2(forecast[12] + ' ' + forecast[13]),
                    'lowertemprature': float(forecast[14]),
                    'uppertemprature': float(forecast[15]),
                    'desc': forecast[16],
                    'desc_code': int(forecast[11])
                },
                {
                    #'time': self.time_format2(forecast[18] + ' ' + forecast[19]),
                    'lowertemprature': float(forecast[20]),
                    'uppertemprature': float(forecast[21]),
                    'desc': forecast[22],
                    'desc_code': int(forecast[17])
                },
                {
                    #'time': self.time_format2(forecast[24] + ' ' + forecast[25]),
                    'lowertemprature': float(forecast[26]),
                    'uppertemprature': float(forecast[27]),
                    'desc': forecast[28],
                    'desc_code': int(forecast[23])
                }
            ]
        else:
            forecast_temp = None
        item['forecast'] = forecast_temp
        yield item


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
        col = get_mongodb('yahooweather', 'CityTemprature', profile=None)
        col.save(data)
        return item

