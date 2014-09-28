# coding=utf-8
import json
import random
import re

import pymongo
from scrapy import Item, Field, Request, Selector
from scrapy.contrib.spiders import CrawlSpider


__author__ = 'zephyre'


class BusStatoinItem(Item):
    # 车站id
    station_id = Field()
    # 城市名称
    city = Field()
    city_id = Field()
    # 车站名称
    name = Field()
    # 车站详情url
    url = Field()
    # 车站地址
    addr = Field()
    # 车站电话
    tel = Field()
    # 是否经过人工验证
    verified = Field()
    # 坐标
    blat = Field()
    blng = Field()


class BusStation(CrawlSpider):
    name = 'bus_station'  # name of spider

    def __init__(self, *a, **kw):
        super(BusStation, self).__init__(*a, **kw)
        self.baidu_key = []

    def start_requests(self):
        yield Request(url='http://cms.lvxingpai.cn/baidu-key.json', callback=self.parse_key)

    def parse_key(self, response):
        self.baidu_key = json.loads(response.body)

        for loc in pymongo.Connection().geo.Locality.find({'level': {'$gte': 2}}, {'shortName': 1, 'zhName': 1}):
            yield Request(url=u'http://qiche.o.cn/qcz/%s' % loc['shortName'],
                          callback=self.parse_city, meta={'data': {'city': loc['zhName'], 'city_id': loc['_id']}})

    def parse_city(self, response):
        for node in Selector(response).xpath('//ul[contains(@class,"c_list_mod5")]/li/h3/a[@href]'):
            url = node.xpath('./@href').extract()[0]
            match = re.search(r'/(\d+)/?', url)
            if not match:
                continue
            station_id = int(match.groups()[0])
            ret = node.xpath('./text()').extract()[0]
            name = ret.strip() if ret else None
            ret = node.xpath('../../table[contains(@class,"c_table_mod1")]//i[@class="i_valide"]/text()').extract()
            verified = (ret and u'人工验证' in ret[0])
            addr = None
            tel = None
            for ret in node.xpath(
                    '../../table[contains(@class,"c_table_mod1")]//td[@class="t2" or @class="t1"]/text()').extract():
                match = re.search(ur'(地址|电话)\s*：\s*(.+?)$', ret)
                if not match:
                    continue
                gr = match.groups()
                if gr[0] == u'地址':
                    addr = gr[1].strip()
                elif gr[0] == u'电话':
                    tel = gr[1].strip()

            item = BusStatoinItem()
            data = response.meta['data']
            item['station_id'] = station_id
            item['city'] = data['city']
            item['city_id'] = data['city_id']
            item['name'] = name
            item['url'] = url
            item['addr'] = addr
            item['tel'] = tel
            item['verified'] = verified

            key = self.baidu_key.values()[random.randint(0, len(self.baidu_key) - 1)]
            yield Request('http://api.map.baidu.com/geocoder/v2/?ak=%s&output=json&address=%s&city=%s' % (
                key, item['name'], item['city']),
                          callback=self.parse_addr, meta={'item': item, 'key': key})

    def parse_addr(self, response):
        loc = json.loads(response.body)['result']['location']
        lat, lng = loc['lat'], loc['lng']
        item = response.meta['item']
        item['blat'] = lat
        item['blng'] = lng
        yield item


class BusStationPipeline(object):
    def process_item(self, item, spider):
        if type(item).__name__ != BusStatoinItem.__name__:
            return item

        sid = item['station_id']
        col = pymongo.Connection().raw_data.BusStation
        entry = col.find_one({'stationId': sid})
        if not entry:
            entry = {}

        entry['stationId'] = sid
        entry['city'] = item['city']
        entry['cityId'] = item['city_id']
        entry['name'] = item['name']
        entry['url'] = item['url']
        entry['addr'] = item['addr']
        entry['tel'] = item['tel']
        entry['verified'] = item['verified']
        entry['blat'] = item['blat']
        entry['blng'] = item['blng']

        col.save(entry)

        return item


# class BusStatoinItem(Item):
#     # 车站id
#     station_id = Field()
#     # 城市名称
#     city = Field()
#     city_id = Field()
#     # 车站名称
#     name = Field()
#     # 车站详情url
#     url = Field()
#     # 车站地址
#     addr = Field()
#     # 车站电话
#     tel = Field()
#     # 是否经过人工验证
#     verified = Field()