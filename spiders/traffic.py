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
    # 省名称
    prov = Field()
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

        yield Request(url='http://qiche.o.cn/allcz', callback=self.parse_prov)

        # for loc in pymongo.Connection().geo.Locality.find({'level': {'$gte': 2}},
        # {'shortName': 1, 'zhName': 1, 'level': 1, 'super': 1}):
        # yield Request(url=u'http://qiche.o.cn/qcz/%s' % loc['shortName'],
        # callback=self.parse_city, meta={'data': {'city': loc['zhName'], 'city_id': loc['_id']}})

    def parse_prov(self, response):
        for node in Selector(response).xpath('//div[@class="bd"]/dl[contains(@class,"c_dl_mod11")]/dd/em/a'):
            prov_name = node.xpath('./text()').extract()[0]
            if prov_name == u'直辖市':
                prov_name = None
            url = node.xpath('./@href').extract()[0]
            yield Request(url=url, meta={'prov': prov_name}, callback=self.parse_city_list)

    def parse_city_list(self, response):
        prov_name = response.meta['prov']
        for node in Selector(response).xpath('//div[@class="bd"]/dl[contains(@class,"c_dl_mod12")]/dd/em/a'):
            city_name = node.xpath('./text()').extract()[0]
            url = node.xpath('./@href').extract()[0] + '_1'
            yield Request(url=url, meta={'page': 1, 'prov': prov_name, 'city': city_name}, callback=self.parse_city)

    def parse_city(self, response):
        sel = Selector(response)
        for node in sel.xpath('//div[@class="bd"]/ul[contains(@class,"c_list_mod5")]/li/h3/a[@href]'):
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
            item['station_id'] = station_id
            item['city'] = response.meta['city']
            item['prov'] = response.meta['prov']
            item['name'] = name
            item['url'] = url
            item['addr'] = addr
            item['tel'] = tel
            item['verified'] = verified

            key = self.baidu_key.values()[random.randint(0, len(self.baidu_key) - 1)]
            yield Request('http://api.map.baidu.com/geocoder/v2/?ak=%s&output=json&address=%s&city=%s' % (
                key, item['name'], item['city']),
                          callback=self.parse_addr, meta={'item': item, 'key': key})

        # 处理分页
        ret = sel.xpath('//div[contains(@class,"pagination")]/span/text()').extract()
        if ret:
            match = re.search(r'\d+/(\d+)', ret[0])
            if match:
                tot_page = int(match.groups()[0])
                cur_page = response.meta['page']
                if cur_page < tot_page:
                    cur_page += 1
                    url = re.sub(r'\d+$', str(cur_page), response.url)
                    yield Request(url=url,
                                  meta={'page': cur_page, 'prov': response.meta['prov'], 'city': response.meta['city']},
                                  callback=self.parse_city)


    def parse_addr(self, response):
        item = response.meta['item']
        try:
            loc = json.loads(response.body)['result']['location']
            lat, lng = loc['lat'], loc['lng']

            item['blat'] = lat
            item['blng'] = lng
            yield item

        except KeyError:
            if 'stop_flag' in response.meta:
                yield item
            else:
                key = self.baidu_key.values()[random.randint(0, len(self.baidu_key) - 1)]
                yield Request('http://api.map.baidu.com/geocoder/v2/?ak=%s&output=json&address=%s&city=%s' % (
                    key, item['addr'], item['city']),
                              callback=self.parse_addr, meta={'item': item, 'key': key, 'stop_flag': True})


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
        entry['prov'] = item['prov']
        entry['name'] = item['name']
        entry['url'] = item['url']
        entry['addr'] = item['addr']
        entry['tel'] = item['tel']
        entry['verified'] = item['verified']
        if 'blat' in item and 'blng' in item:
            entry['blat'] = item['blat']
            entry['blng'] = item['blng']
        else:
            entry['blat'] = None
            entry['blng'] = None

        col.save(entry)

        return item
