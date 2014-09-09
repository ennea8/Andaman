import random

__author__ = 'zwh'
import json

import scrapy
import pymongo
from scrapy import Request
from scrapy.contrib.spiders import CrawlSpider
import conf


class WeatherItem(scrapy.Item):
    # id = scrapy.Field()
    # superAdm_id = scrapy.Field()
    # superAdm_name = scrapy.Field()
    # county = scrapy.Field()
    data = scrapy.Field()
    loc = scrapy.Field()


class WeatherBaiduSpider(CrawlSpider):
    name = 'baidu_weather'

    def __init__(self, *a, **kw):
        super(WeatherBaiduSpider, self).__init__(*a, **kw)
        self.item_class = WeatherItem

    def start_requests(self):
        col = pymongo.MongoClient().geo.Locality
        all_obj = list(col.find({"level": {"$in": [2, 3]}}, {"zhName": 1}))
        ak_list = conf.global_conf['baidu-key'].values() if 'baidu-key' in conf.global_conf else []

        for county_code in all_obj:
            m = {"county_name": county_code['zhName'], "county_id": county_code["_id"]}

            idx = random.randint(0, len(ak_list) - 1)
            ak = ak_list[idx]

            yield Request(url='http://api.map.baidu.com/telematics/v3/weather?location=%s&output='
                              'json&ak=%s' % (county_code['zhName'], ak),
                          callback=self.parse, meta={'WeatherData': m})

    def parse(self, response):
        data = json.loads(response.body, encoding='utf-8')
        if data['error'] != 0:
            return

        allInf = response.meta['WeatherData']
        item = WeatherItem()
        item['data'] = data['results'][0]
        item['loc'] = {'id': allInf['county_id'],
                       'zhName': allInf['county_name']}

        # item['superAdm_id'] = allInf["superAdm_id"]
        # item['superAdm_name'] = allInf['superAdm_name']
        # item['county'] = allInf['county_name']
        # item['id'] = allInf['county_id']

        return item


class WeatherPipeline(object):
    def __init__(self):
        ret = conf.global_conf['weather']
        # self.host = ret['host']
        # self.port = int(ret['port'])

    def process_item(self, item, spider):
        if not isinstance(item, spider.item_class):
            return item
        weather_entry = {'loc': item['loc']}
        for k in item['data']:
            weather_entry[k] = item['data'][k]

        # col = pymongo.MongoClient(host=self.host, port=self.port).misc.Weather
        col = pymongo.MongoClient().misc.Weather
        ret = col.find_one({'loc.id': item['loc']['id']}, {'_id': 1})
        if ret:
            weather_entry['_id'] = ret['_id']

        col.save(weather_entry)
        return item