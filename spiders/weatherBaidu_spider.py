import random

from items import WeatherItem


__author__ = 'zwh'
import json

import pymongo
from scrapy import Request
from scrapy.contrib.spiders import CrawlSpider
import conf
from scrapy import log


class WeatherBaiduSpider(CrawlSpider):
    name = 'baidu_weather'

    def __init__(self, *a, **kw):
        super(WeatherBaiduSpider, self).__init__(*a, **kw)

    def start_requests(self):
        col = pymongo.MongoClient().geo.Locality
        all_obj = list(col.find({"level": {"$in": [2, 3]}}, {"zhName": 1, 'coords': 1}))
        ak_list = conf.global_conf['baidu-key'].values() if 'baidu-key' in conf.global_conf else []

        for county_code in all_obj:
            m = {"county_name": county_code['zhName'], "county_id": county_code["_id"]}

            idx = random.randint(0, len(ak_list) - 1)
            ak = ak_list[idx]

            s = None
            if 'coords' in county_code:
                coords = county_code['coords']
                if 'blat' in coords and 'blng' in coords:
                    s = '%f,%f' % (coords['blng'], coords['blat'])
                elif 'lat' in coords and 'lng' in coords:
                    s = '%f,%f' % (coords['lng'], coords['lat'])
            if not s:
                s = county_code['zhName']
            yield Request(url='http://api.map.baidu.com/telematics/v3/weather?location=%s&output='
                              'json&ak=%s' % (s, ak), callback=self.parse, meta={'WeatherData': m})

    def parse(self, response):
        try:
            data = json.loads(response.body, encoding='utf-8')
            if data['status'] != 'success':
                self.log('ERROR PARSING: %s, RESULT=%s' % (response.url, response.body), level=log.WARNING)
                return
        except ValueError:
            self.log('ERROR PARSING: %s, RESULT=%s' % (response.url, response.body), level=log.WARNING)
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
    def process_item(self, item, spider):
        if not isinstance(item, WeatherItem):
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