__author__ = 'zwh'
import scrapy
import json
import pymongo

from scrapy import Request
from scrapy.contrib.spiders import CrawlSpider

class WeatherItem(scrapy.Item):
    # define the fields for your item here like:
    id = scrapy.Field()
    superAdm_id = scrapy.Field()
    superAdm_name = scrapy.Field()
    county = scrapy.Field()
    data = scrapy.Field()

class WeatherBaiduSpider(CrawlSpider):

    def __init__(self, *a, **kw):
        self.name = 'weather'
        super(WeatherBaiduSpider, self).__init__(*a, **kw)

    def start_requests(self):
        client = pymongo.MongoClient()
        self.geo = client.geo
        # county_obj = self.geo.Locality.find({"level":3},{"zhName":1,"superAdm.zhName":1})
        # city_obj = self.geo.Locality.find({"level":2},{"zhName":1,"superAdm.zhName":1})
        all_obj=self.geo.Locality.find({"level":{"$in":[2,3]}},{"zhName":1,"superAdm":1})
        for county_code in all_obj:
            m ={}
            m["superAdm_name"] = county_code["superAdm"]["zhName"]
            m["superAdm_id"] = county_code["superAdm"]["id"]
            m["county_name"] = county_code['zhName']
            m["county_id"] = county_code["_id"]
            yield Request(url='http://api.map.baidu.com/telematics/v3/weather?location=%s&output='
                              'json&ak=qLaEdfHSTiL9iMAAibIOAH0V'%county_code['zhName'],
                          callback=self.parse,meta={'WeatherData': m})

    def parse(self, response):
        allInf = response.meta['WeatherData']
        item = WeatherItem()
        item['data'] = json.loads(response.body, encoding='utf-8')
        item['superAdm_id'] = allInf["superAdm_id"]
        item['superAdm_name'] = allInf['superAdm_name']
        item['county'] = allInf['county_name']
        item['id'] = allInf['county_id']
        yield item


class WeatherPipeline(object):

    def __init__(self):
        client = pymongo.MongoClient('localhost', 27017)
        self.db = client.misc

    def process_item(self, item, spider):
        # log.msg("Sving", level=log.INFO)
        countyId = item['id']
        superAdmId = item['superAdm_id']
        superAdmName = item['superAdm_name']
        county = item['county']
        data =item['data']
        # if data['error']==0:

        weather_entry = {'localityId': countyId,'Data': data,
                                   'superAdm':{'id': superAdmId, 'zhName': superAdmName}, 'county_or_city': county}

        weather_id = self.db.Weather.find_one({'localityId':countyId})
        if weather_id:
            weather_entry['_id'] = weather_id['_id']
        if data['error']!=0:
            self.db.weather.save(weather_entry)
        return item