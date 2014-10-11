# coding=utf-8
import json
import re

import pymongo
from scrapy import Request, Selector, Item, Field, log
from scrapy.contrib.spiders import CrawlSpider

import utils


__author__ = 'zephyre'


class QyerCountryItem(Item):
    # 国家id
    country_id = Field()
    # 国家中文名称
    country_zh = Field()
    # 国家英文名称
    country_en = Field()
    # 洲中文名称
    cont_zh = Field()
    # 洲英文名称
    cont_en = Field()
    # 是否为热本国家
    is_hot = Field()
    url = Field()


class QyerLocItem(Item):
    # 国家id
    country_id = Field()
    # 国家英文名称
    country_en = Field()
    # 洲中文名称
    cont_zh = Field()
    # 洲英文名称
    cont_en = Field()
    # 是否为热本国家
    is_hot = Field()
    url = Field()


class QyerCountrySpider(CrawlSpider):
    name = 'qyer_countries'  # name of spider

    def __init__(self, *a, **kw):
        super(QyerCountrySpider, self).__init__(*a, **kw)

    def start_requests(self):
        yield Request(url='http://place.qyer.com', callback=self.parse)

    def parse(self, response):
        for node in Selector(response).xpath('//div[contains(@class,"pla_indcountrylist") and @id]'):
            ret = node.xpath('./h2[contains(@class,"title")]/em/a/text()').extract()
            if not ret:
                continue
            ret = re.split(r'\s+', ret[0])
            cn_name = ret[0]
            en_name = ' '.join(ret[1:])

            for node_c in node.xpath('./div[contains(@class,"line")]/ul/li[@class="item"]//a[@data-bn-ipg]'):
                url = node_c.xpath('./@href').extract()[0]
                match = re.search(r'place-index-countrylist-(\d+)', node_c.xpath('./@data-bn-ipg').extract()[0])
                if not match:
                    continue
                country_id = int(match.groups()[0])
                ret = node_c.xpath('./text()').extract()
                if not ret:
                    continue
                country_zh = ret[0].strip()
                ret = node_c.xpath('./span[@class="en"]/text()').extract()
                if not ret:
                    continue
                country_en = ret[0].strip()
                ret = node_c.xpath('../../p[contains(@class,"hot")]')
                is_hot = bool(ret)

                item = QyerCountryItem()
                item['country_id'] = country_id
                item['country_zh'] = country_zh
                item['country_en'] = country_en
                item['cont_zh'] = cn_name
                item['cont_en'] = en_name
                item['is_hot'] = is_hot
                item['url'] = url

                yield item


class QyerCountryPipeline(object):
    spiders = [QyerCountrySpider.name]

    def process_item(self, item, spider):
        if type(item).__name__ != QyerCountryItem.__name__:
            return item

        cid = item['country_id']
        col = pymongo.Connection().raw_data.QyerCountry
        # NoSQL
        entry = col.find_one({'countryId': cid})
        if not entry:
            entry = {}

        entry['countryId'] = cid
        entry['zhName'] = item['country_zh']
        entry['enName'] = item['country_en']
        entry['zhContinent'] = item['cont_zh']
        entry['enContinent'] = item['cont_en']
        entry['isHot'] = item['is_hot']
        entry['url'] = item['url']

        col.save(entry)
        return item


class QyerCountryProcSpider(CrawlSpider):
    """
    对穷游的国家数据进行清洗
    """
    name = 'qyer_countries_proc'  # name of spider

    def __init__(self, *a, **kw):
        super(QyerCountryProcSpider, self).__init__(*a, **kw)

    def start_requests(self):
        yield Request(url='http://www.baidu.com', callback=self.parse)

    def parse(self, response):
        for entry in pymongo.Connection().raw_data.QyerCountry.find({}):
            item = QyerCountryItem()
            item['country_id'] = entry['countryId']
            item['country_zh'] = entry['zhName']
            item['country_en'] = entry['enName']
            item['cont_zh'] = entry['zhContinent']
            item['cont_en'] = entry['enContinent']
            item['is_hot'] = entry['isHot']

            yield item


class QyerCountryProcPipeline(object):
    """
    对穷游的国家数据进行清洗
    """

    spiders = [QyerCountryProcSpider.name]

    def process_item(self, item, spider):
        if type(item).__name__ != QyerCountryItem.__name__:
            return item

        col = pymongo.Connection().geo.Country
        zh_name = item['country_zh']
        entry = col.find_one({'zhName': zh_name})
        if not entry:
            entry = {}

        entry['zhName'] = zh_name
        entry['enName'] = item['country_en']
        entry['qyerId'] = item['country_id']
        entry['zhCont'] = item['cont_zh']
        entry['enCont'] = item['cont_en']
        entry['isHot'] = item['is_hot']

        col.save(entry)

        return item


class CityItem(Item):
    # 城市ID
    city_id = Field()
    # 城市名称
    en_name = Field()
    zh_name = Field()
    # 纬度
    lat = Field()
    # 经度
    lng = Field()
    # 国家名称
    en_country = Field()
    zh_country = Field()
    # 国家代码
    country_code = Field()
    # 别名
    alias = Field()
    # 人口
    population = Field()


class GeoNamesProcSpider(CrawlSpider):
    """
    处理GeoNames的城市数据
    """

    name = 'geonames_proc'

    country_map = {}
    missed_countries = set([])

    def __init__(self, *a, **kw):
        super(GeoNamesProcSpider, self).__init__(*a, **kw)

    def start_requests(self):
        param = getattr(self, 'param', {'countries': []})
        yield Request(url='http://www.baidu.com', meta={'countries': param['countries']}, callback=self.parse)

    def parse(self, response):
        col = utils.get_mongodb('raw_data', 'GeoNamesCity', profile='mongodb-crawler')
        countries = response.meta['countries']
        for entry in list(
                col.find({'$or': [{'countryCode': tmp.upper()} for tmp in countries]} if countries else {},
                         {'_id': 1})):
            city = col.find_one({'_id': entry['_id']})

            item = CityItem()
            item['city_id'] = city['_id']
            en_name = city['enName'].lower().strip()
            zh_name = city['zhName'].lower().strip()
            item['en_name'] = en_name
            item['zh_name'] = zh_name
            s = set([tmp.lower().strip() for tmp in (item['alias'] if 'alias' in city else [])])
            s.add(en_name)
            s.add(zh_name)
            item['alias'] = list(s)
            item['lat'] = city['lat']
            item['lng'] = city['lng']
            item['population'] = city['population']

            country_code = city['countryCode'].lower()
            item['country_code'] = country_code
            if country_code in GeoNamesProcSpider.country_map:
                country = GeoNamesProcSpider.country_map[country_code]
            elif country_code not in GeoNamesProcSpider.missed_countries:
                col_country = utils.get_mongodb('geo', 'Country', profile='mongodb-general')
                country = col_country.find_one({'code': country_code})
                if not country:
                    self.log('MISSED COUNTRY: %s' % country_code, log.WARNING)
                    GeoNamesProcSpider.missed_countries.add(country_code)
                    continue
                else:
                    GeoNamesProcSpider.country_map[country_code] = country
            else:
                continue
            item['en_country'] = country['enName']
            item['zh_country'] = country['zhName']

            yield Request(url='http://maps.googleapis.com/maps/api/geocode/json?address=%s,%s&sensor=false' % (
                item['en_name'], item['en_country']), callback=self.parse_geocode, meta={'item': item, 'lang': 'zh'},
                          headers={'Accept-Language': 'zh-CN'}, dont_filter=True)

    def parse_geocode(self, response):
        item = response.meta['item']
        lang = response.meta['lang']
        try:
            data = json.loads(response.body)['results'][0]['address_components'][0]
            short_name = data['short_name'].lower()
            long_name = data['long_name'].lower()
            s = set(item['alias'])
            s.add(short_name)
            s.add(long_name)
            k = 'zh_name' if lang == 'zh' else 'en_name'
            s.add(item[k])
            item[k] = long_name
            item['alias'] = list(s)
        except (KeyError, IndexError):
            self.log('ERROR GEOCODEING: %s' % response.url, log.WARNING)

        if lang == 'zh':
            yield Request(url='http://maps.googleapis.com/maps/api/geocode/json?address=%s,%s&sensor=false' % (
                item['en_name'], item['en_country']), callback=self.parse_geocode, meta={'item': item, 'lang': 'en'},
                          headers={'Accept-Language': 'en-US'}, dont_filter=True)
        else:
            yield item


class GeoNamesProcPipeline(object):
    """
    处理GeoNames的城市数据
    """

    spiders = [GeoNamesProcSpider.name]

    country_map = {}

    def process_item(self, item, spider):
        if type(item).__name__ != CityItem.__name__:
            return item

        col_loc = utils.get_mongodb('geo', 'Locality', profile='mongodb-general')

        city_id = item['city_id']
        city = col_loc.find_one({'misc.geoNamesId': city_id})
        if not city:
            city = {}

        city['enName'] = item['en_name']
        city['zhName'] = item['zh_name']
        city['alias'] = item['alias']
        city['shortName'] = utils.get_short_loc(item['zh_name'])
        city['pinyin'] = []

        country_code = item['country_code']
        if country_code not in GeoNamesProcPipeline.country_map:
            col_country = utils.get_mongodb('geo', 'Country', profile='mongodb-general')
            country = col_country.find_one({'code': country_code})
            GeoNamesProcPipeline.country_map[country_code] = country
        else:
            country = GeoNamesProcPipeline.country_map[country_code]
        city['country'] = {'id': country['_id'], 'enName': country['enName'], 'zhName': country['zhName']}

        city['level'] = 3
        city['images'] = []
        city['imageList'] = []
        city['coords'] = {'lat': item['lat'], 'lng': item['lng']}
        if 'misc' not in city:
            city['misc'] = {}
        city['misc']['geoNamesId'] = city_id
        city['misc']['geoNamesPopulation'] = item['population']
        city['abroad'] = True

        col_loc.save(city)

        return item


class GeoNamesSpider(CrawlSpider):
    name = 'geonames'

    def __init__(self, *a, **kw):
        super(GeoNamesSpider, self).__init__(*a, **kw)

    @staticmethod
    def xfrange(start, stop, step):
        while start < stop:
            yield start
            start += step

    def start_requests(self):
        south = -90
        north = 90
        west = -180
        east = 180
        delta = 5
        max_row = 30

        def func(val):
            match = re.search(r'm(\d+)', val)
            return -float(match.groups()[0]) if match else float(val)

        if 'param' in dir(self):
            param = getattr(self, 'param')
            if 'south' in param:
                south = func(param['south'][0])
            if 'north' in param:
                north = func(param['north'][0])
            if 'west' in param:
                west = func(param['west'][0])
            if 'east' in param:
                east = func(param['east'][0])
            if 'delta' in param:
                delta = float(param['delta'][0])
            if 'mr' in param:
                max_row = int(param['mr'][0])

        for lat in self.xfrange(south, north, delta):
            for lng in self.xfrange(west, east, delta):
                url = 'http://api.geonames.org/citiesJSON?north=%f&south=%f&east=%f&west=%f&lang=zh&username=zephyre&maxRows=%d' % (
                    lat + delta, lat, lng + delta, lng, max_row
                )
                yield Request(url=url, callback=self.parse)

    def parse(self, response):
        data = json.loads(response.body)
        if 'geonames' not in data:
            return

        for entry in data['geonames']:
            country_code = entry['countrycode']
            if country_code.lower() in ['cn', 'mo', 'hk', 'tw']:
                continue

            en_name = entry['toponymName']
            zh_name = entry['name']
            lat = entry['lat']
            lng = entry['lng']
            city_id = entry['geonameId']
            population = entry['population']

            item = CityItem()
            item['en_name'] = en_name
            item['zh_name'] = zh_name
            item['lat'] = lat
            item['lng'] = lng
            item['country_code'] = country_code
            item['population'] = population
            item['city_id'] = city_id

            yield item


class TravelGisSpider(CrawlSpider):
    name = 'travel_gis'  # name of spider

    def __init__(self, *a, **kw):
        super(TravelGisSpider, self).__init__(*a, **kw)

    def start_requests(self):
        lower = 1
        upper = 94
        if 'param' in dir(self):
            param = getattr(self, 'param')
            if 'lower' in param:
                lower = int(param['lower'][0])
            if 'upper' in param:
                upper = int(param['upper'][0])

        for page in xrange(lower, upper):
            yield Request(url='http://www.travelgis.com/world/ppla.asp?pagenumber=%d' % page, callback=self.parse)

    def parse(self, response):
        for node in Selector(response).xpath('//table[@width and @cellpadding]/tr[@bgcolor]'):
            try:
                node_list = node.xpath('./td/a[@href]')
                if len(node_list) != 4:
                    continue
                city = node_list[0].xpath('./text()').extract()[0]
                match = re.findall(r'(lon|lat)=(-?[\d\.]+)', node_list[0].xpath('./@href').extract()[0])
                if match:
                    coords = dict(match)
                    lat = float(coords['lat'])
                    lng = float(coords['lon'])
                else:
                    lat = None
                    lng = None
                country = node_list[2].xpath('./text()').extract()[0]
                match = re.search(r'/(\w{2})/', node_list[2].xpath('./@href').extract()[0])
                code = match.groups()[0].lower() if match else None

                item = CityItem()
                item['country'] = country
                item['city'] = city
                item['lat'] = lat
                item['lng'] = lng
                item['code'] = code

                yield item
            except IndexError:
                continue


class TravelGisPipeline(object):
    """
    从TravelGis获得的原始城市数据
    """

    spiders = [TravelGisSpider.name]

    def process_item(self, item, spider):
        if type(item).__name__ != CityItem.__name__:
            return item

        col = utils.get_mongodb('raw_data', 'TravelGisCity', 'localhost', 27027)

        ret = col.find_one({'name': item['city'], 'countryCode': item['code']})
        if not ret:
            ret = {}

        ret['name'] = item['city']
        ret['country'] = item['country']
        ret['countryCode'] = item['code']
        ret['lat'] = item['lat']
        ret['lng'] = item['lng']

        col.save(ret)

        return item


class GeoNamesPipeline(object):
    """
    从GeoNames获得的原始城市数据
    """

    spiders = [GeoNamesSpider.name]

    def process_item(self, item, spider):
        if type(item).__name__ != CityItem.__name__:
            return item

        col = utils.get_mongodb('raw_data', 'GeoNamesCity', 'localhost', 27027)

        ret = col.find_one({'_id': item['city_id']})
        if not ret:
            ret = {}

        ret['enName'] = item['en_name']
        ret['zhName'] = item['zh_name']
        ret['countryCode'] = item['country_code']
        ret['lat'] = item['lat']
        ret['lng'] = item['lng']
        ret['population'] = item['population']
        ret['_id'] = item['city_id']

        col.save(ret)

        return item