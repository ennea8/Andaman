# coding: utf-8
import urlparse

import scrapy

import utils


__author__ = 'zwh'
import copy
import re
import json

from scrapy import Request, Selector, log
from scrapy.contrib.spiders import CrawlSpider


class QyerAlienpoiItem(scrapy.Item):
    country_info = scrapy.Field()
    poi_url = scrapy.Field()
    poi_id = scrapy.Field()
    poi_cover = scrapy.Field()
    poi_name = scrapy.Field()
    poi_score = scrapy.Field()
    poi_englishName = scrapy.Field()
    poi_summary = scrapy.Field()
    poi_detail = scrapy.Field()
    poi_photo = scrapy.Field()
    poi_been = scrapy.Field()
    poi_lat = scrapy.Field()
    poi_lng = scrapy.Field()
    poi_city = scrapy.Field()


class QyerSpotSpider(CrawlSpider):
    name = 'qyer_spot'

    def __init__(self, *a, **kw):
        super(QyerSpotSpider, self).__init__(*a, **kw)
        self.param = {}

    def start_requests(self):
        self.param = getattr(self, 'param', {})

        if 'country' in self.param:
            self.param['country'] = [tmp.lower() for tmp in self.param['country']]

        continent_list = ["asia", "europe", "africa", "north-america", "south-america",
                          "oceania"] if 'region' not in self.param else [tmp.lower() for tmp in self.param['region']]

        for continent in continent_list:
            url = 'http://place.qyer.com/%s' % continent
            yield Request(url=url, callback=self.parse_homepage)

    def parse_homepage(self, response):
        sel = Selector(response)

        def func(node, hot):
            country_url = node.xpath('./@href').extract()[0].strip()
            country_name = node.xpath('./text()').extract()[0].strip()
            ret = node.xpath('./span[@class="en"]/text()').extract()
            country_engname = ret[0].lower().strip() if ret else None

            if 'country' in self.param and country_engname not in self.param['country']:
                return None

            sights_url = urlparse.urljoin(country_url, './sight')
            m = {"country_name": country_name, "country_url": country_url, "country_popular": hot,
                 "country_engname": country_engname, "sights_url": sights_url}
            return Request(url=sights_url, callback=self.parse_countrysights, meta={"country": m})

        for req in map(lambda node: func(node, False),
                       sel.xpath('//div[@id="allcitylist"]/div[contains(@class,"line")]/ul/li/a[@href]')):
            yield req

        for req in map(lambda node: func(node, True),
                       sel.xpath(
                               '//div[@id="allcitylist"]/div[contains(@class,"line")]/ul/li/p[@class="hot"]/a[@href]')):
            yield req

    def parse_countrysights(self, response):
        sel = Selector(response)
        country = response.meta["country"]

        tmp = sel.xpath(
            '//div[@id="place_memu_fix"]/div/div[@class="pla_topbtns"]/a[@class="ui_button yelp"]/@onclick').extract()[
            0]
        country_id = int(re.search(r'[0-9]+', tmp).group())

        tmp = sel.xpath('//div/ul[@id="tab"]/li/a[@data-id="allpoiContent"]/text()').extract()
        if tmp:
            num = int(re.search(r'[0-9]+', tmp[0]).group())
            pagenum = int(num / 16) + 1

            for page in range(pagenum):
                country_info = copy.deepcopy(country)
                for tp in ('city', 'country'):
                    body = 'action=ajaxpoi&page=%d&pagesize=16&id=%d&typename=%s&cateid=32&orderby=0&tagid=0' % (
                        page + 1, country_id, tp)
                    yield Request(url="http://place.qyer.com/ajax.php", method='POST', body=body,
                                  headers={'Content-Type': 'application/x-www-form-urlencoded',
                                           'X-Requested-With': 'XMLHttpRequest'},
                                  callback=self.parse_list,
                                  meta={"country": country_info}, dont_filter=True)

    def parse_list(self, response):
        country = response.meta["country"]
        data = json.loads(response.body)
        sel = Selector(text=data['data']['html'])
        sights = sel.xpath('//ul/li')
        for sight in sights:
            country_info = copy.deepcopy(country)
            m = {"country_info": country_info}
            tmp = sight.xpath('./p/a/@href').extract()
            if tmp:
                m["poi_url"] = tmp[0]
                m["poi_id"] = re.search(r'/([0-9]+)/', m["poi_url"]).groups()[0]
            else:
                m["poi_url"] = None
                m["poi_id"] = None
            tmp = sight.xpath('./p/a/img/@src').extract()
            if tmp:
                m["poi_cover"] = tmp[0]
            else:
                m["poi_cover"] = None
            tmp = sight.xpath('./h3/a/text()').extract()
            if tmp:
                m["poi_name"] = tmp[0]
            else:
                m['poi_name'] = None

            tmp = sight.xpath('./div/p[@class="score"]/text()').extract()
            m["poi_score"] = None
            if tmp:
                tmp = tmp[0]
                match = re.search('^\s*\d+(\.\d+)?', tmp)
                if match:
                    m["poi_score"] = float(match.group())

            m["poi_been"] = 0
            tmp = sight.xpath('./div/p[@class="been"]/text()').extract()
            if tmp:
                tmp = tmp[0]
                match = re.search('^\s*\d+', tmp)
                if match:
                    m["poi_been"] = int(match.group())

            if m["poi_url"]:
                yield Request(url=m["poi_url"], callback=self.parse_poi, meta={"poi_info": m})

    def parse_poi(self, response):
        sel = Selector(response)
        poi_info = response.meta["poi_info"]

        poi_info['lat'] = None
        poi_info['lng'] = None

        tmp = sel.xpath('//div[@class="wrap"]/a/div[@class="map"]/img[@src]/@src').extract()
        if tmp:
            match = re.search(r'\|(\-?\d+\.\d+),\s*(\-?\d+\.\d+)', tmp[0])
            if match:
                poi_info['lat'] = float(match.groups()[0])
                poi_info['lng'] = float(match.groups()[1])

        # tmp = sel.xpath(
        # '//div[contains(@class,"pla_main")]/div[contains(@class,"pla_textedit")]/a[@href and @onclick]/@onclick').extract()
        # if tmp:
        # tmp = tmp[0]
        # match = re.search(r'^\s*createWindow\(\d+\s*,\s*\d+\s*,\s*([\d\.]+)\s*,\s*([\d\.]+)\s*', tmp)
        # if match:
        # poi_info['lat'] = float(match.groups()[0])
        # poi_info['lng'] = float(match.groups()[1])

        # tmp = sel.xpath('//div[@class="pla_topbars"]/div/div/div[@class="pla_topbar_names"]/p/a/text()').extract()
        tmp = sel.xpath('//div[@class="poiDet-largeTit"]/h1[@class="en"]/a[@href]/text()').extract()
        poi_info["poi_englishName"] = tmp[0].strip() if tmp else None
        # tmp = sel.xpath('//div[@class="pla_main"]/div[@id="summary_fixbox"]/div[@id="summary_box"]/p/text()').extract()
        tmp = sel.xpath('//div[@class="poiDet-largeTit"]/h1[@class="cn"]/a[@href]/text()').extract()
        poi_info['poi_name'] = tmp[0].strip() if tmp else None

        tmp = sel.xpath('//div[@class="poiDet-main"]/div[@class="poiDet-detail"]/descendant-or-self::text()').extract()
        poi_info["poi_summary"] = '\n'.join(filter(lambda val: val, [tmp.strip() for tmp in sel.xpath(
            '//div[@class="poiDet-main"]/div[@class="poiDet-detail"]/descendant-or-self::text()').extract()]))
        # mp])) tmp[0].strip() if tmp else None

        detail = []
        for node in sel.xpath('//div[@class="poiDet-main"]/ul[@class="poiDet-tips"]/li'):
            tmp = '\n'.join(
                filter(lambda val: val, [tmp.strip() for tmp in
                                         node.xpath('./span[@class="title"]/descendant-or-self::text()').extract()]))
            key = tmp
            tmp = '\n'.join(filter(lambda val: val,
                                   [tmp.strip() for tmp in
                                    node.xpath('./div[@class="content"]/descendant-or-self::text()').extract()]))
            val = tmp
            if not key or not val:
                continue
            detail.append({'title': key, 'content': val})

        poi_info["poi_detail"] = detail
        poi_url = poi_info["poi_url"]
        poi_photo_url = poi_url + "/photo"
        yield Request(url=poi_photo_url, callback=self.parse_photo, meta={"poi_info": poi_info})

    def parse_photo(self, response):
        item = QyerAlienpoiItem()
        sel = Selector(response)
        poi_info = response.meta["poi_info"]
        item["country_info"] = poi_info["country_info"]
        item["poi_url"] = poi_info["poi_url"]
        item["poi_id"] = int(poi_info["poi_id"])
        item["poi_cover"] = poi_info["poi_cover"]
        item["poi_name"] = poi_info["poi_name"]
        item["poi_score"] = poi_info["poi_score"]
        item["poi_englishName"] = poi_info["poi_englishName"]
        item["poi_summary"] = poi_info["poi_summary"]
        item["poi_detail"] = poi_info["poi_detail"]
        item["poi_been"] = poi_info['poi_been']
        item['poi_lat'] = poi_info['lat']
        item['poi_lng'] = poi_info['lng']
        item['poi_photo'] = sel.xpath(
            '//div/ul[contains(@class, "pla_photolist")]/li/p[@class="pic"]/a/img/@src').extract()
        yield item


class QyerSpotPipeline(object):
    spiders = [QyerSpotSpider.name]

    def process_item(self, item, spider):
        col = utils.get_mongodb('raw_data', 'QyerSpot', profile='mongodb-crawler')
        data = col.find_one({'poi_id': item['poi_id']})
        if not data:
            data = {}
        for key in item.keys():
            data[key] = item[key]
        col.save(data)
        return item


class QyerSpotProcSpider(CrawlSpider):
    """
    处理穷游的景点数据
    """

    name = 'qyer_spot_proc'

    def __init__(self, *a, **kw):
        super(QyerSpotProcSpider, self).__init__(*a, **kw)

    def start_requests(self):
        param = getattr(self, 'param', {})
        countries = param['country'] if 'country' in param else []
        yield Request(url='http://www.baidu.com', meta={'countries': countries}, callback=self.parse)

    def parse(self, response):
        meta = response.meta
        col_raw = utils.get_mongodb('raw_data', 'QyerSpot', profile='mongodb-crawler')

        for country in meta['countries']:
            for entry in col_raw.find({'country_info.country_engname': country}):
                lat = entry['poi_lat']
                lng = entry['poi_lng']

                if not lat or not lng:
                    continue

                item = QyerAlienpoiItem()
                for k in entry:
                    if k in item.fields:
                        item[k] = entry[k]

                url = 'http://maps.googleapis.com/maps/api/geocode/json?address=%f,%f' % (lat, lng)
                yield Request(url=url, meta={'item': item}, callback=self.parse_geocode)

    def parse_geocode(self, response):
        geocode_ret = json.loads(response.body)
        components = geocode_ret['results'][0]['address_components']

        # find locality
        tmp = filter(lambda entry: 'locality' in entry['types'], components)
        if not tmp:
            self.log('Failed to find locality from Geocode: %s' % response.url, log.WARNING)
            return
        city_name = tmp[0]['long_name']

        # find country
        tmp = filter(lambda entry: 'country' in entry['types'], components)
        if not tmp:
            self.log('Failed to find country from Geocode: %s' % response.url, log.WARNING)
            return
        country_name = tmp[0]['long_name']

        item = response.meta['item']
        item['poi_city'] = city_name
        item['country_info'] = country_name
        yield item


class QyerSpotProcPipeline(object):
    spiders = [QyerSpotProcSpider.name]

    def process_item(self, item, spider):
        country_name = item['country_info']
        city_name = item['poi_city']

        # lookup the country
        col_country = utils.get_mongodb('geo', 'Country', profile='mongodb-general')
        ret = col_country.find_one({'alias': country_name.lower()}, {'zhName': 1, 'enName': 1})
        if not ret:
            spider.log('Failed to find country: %s' % country_name, log.WARNING)
            return
        country_info = {'id': ret['_id'], '_id': ret['_id'], 'zhName': ret['zhName'], 'enName': ret['enName']}

        # lookup the city
        col_loc = utils.get_mongodb('geo', 'Locality', profile='mongodb-general')
        ret = col_loc.find_one({'country.id': country_info['_id'], 'alias': city_name.lower()}, {'zhName': 1, 'enName': 1})
        if not ret:
            spider.log('Failed to find locality from DB: %s' % city_name, log.WARNING)
            return
        city_info = {'id': ret['_id'], '_id': ret['_id'], 'zhName': ret['zhName'], 'enName': ret['enName']}

        # lookup the poi
        col_vs = utils.get_mongodb('poi', 'ViewSpot', profile='mongodb-general')
        vs = col_vs.find_one({'source.qyer.id': item['poi_id']})
        if not vs:
            vs = {}

        source = vs['source'] if 'source' in vs else {}
        source['qyer'] = {'id': item['poi_id'], 'url': item['poi_url']}
        vs['source'] = source

        desc = vs['description'] if 'description' in vs else {}
        desc['desc'] = item['poi_summary']
        vs['description'] = desc

        vs['name'] = item['poi_name']
        vs['zhName'] = item['poi_name']
        vs['enName'] = item['poi_englishName']
        vs['imageList'] = item['poi_photo']

        vs['addr'] = {'loc': city_info, 'coords': {'lat': item['poi_lat'], 'lng': item['poi_lng']}}
        vs['country'] = country_info

        vs['alias'] = list(set([vs[k].lower() for k in ['name', 'zhName', 'enName']]))
        vs['targets'] = [city_info['_id'], country_info['_id']]
        vs['enabled'] = True

        col_vs.save(vs)

        return item




