# coding=utf-8
import json
import re

import pymongo
from scrapy import Request, Selector, Item, Field, log
from scrapy.contrib.spiders import CrawlSpider

import utils
from utils.database import get_mongodb


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
        for entry in get_mongodb('raw_data', 'QyerCountry', profile='mongodb-crawler'):
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
    # featureCode
    level = Field()
    url = Field()
    desc = Field()
    imageList = Field()
    is_hot = Field()


class GeoNamesProcSpider(CrawlSpider):
    """
    处理GeoNames的城市数据
    """

    name = 'geonames-proc'

    country_map = {}
    missed_countries = set([])

    def __init__(self, *a, **kw):
        super(GeoNamesProcSpider, self).__init__(*a, **kw)

    def start_requests(self):
        param = getattr(self, 'param', {})
        if 'country' not in param:
            param['country'] = []
        yield Request(url='http://www.baidu.com', meta={'country': param['country']}, callback=self.parse)

    def parse(self, response):
        col = get_mongodb('raw_data', 'GeoNames', profile='mongodb-crawler')
        countries = response.meta['country']

        query = {'featureClass': 'P', 'population': {'$gt': 0}}
        if countries:
            if len(countries) > 1:
                query['$or'] = [{'country': tmp.upper()} for tmp in countries]
            else:
                query['country'] = countries[0].upper()
        for entry in col.find(query):
            # city = col.find_one({'_id': entry['_id']})
            city = entry

            item = CityItem()
            item['city_id'] = city['_id']
            item['en_name'] = city['asciiName']
            item['zh_name'] = city['enName']

            item['lat'] = city['lat']
            item['lng'] = city['lng']
            item['population'] = city['population']
            item['level'] = city['featureCode']

            s = set([tmp.lower().strip() for tmp in (item['alias'] if 'alias' in city else [])])
            s.add(city['asciiName'].lower())
            s.add(city['enName'].lower())
            for val in city['altName']:
                s.add(val.lower())
            item['alias'] = list(s)

            country_code = city['country']
            item['country_code'] = country_code
            if country_code in GeoNamesProcSpider.country_map:
                country = GeoNamesProcSpider.country_map[country_code]
            elif country_code not in GeoNamesProcSpider.missed_countries:
                col_country = get_mongodb('geo', 'Country', profile='mongodb-general')
                country = col_country.find_one({'code': country_code})
                if not country:
                    self.log('MISSED COUNTRY: %s' % country_code, log.WARNING)
                    GeoNamesProcSpider.missed_countries.add(country_code)
                    continue
                else:
                    GeoNamesProcSpider.country_map[country_code] = country
            else:
                continue
            item['en_country'] = country['enName'] if 'enName' in country else None
            item['zh_country'] = country['zhName'] if 'zhName' in country else None

            yield Request(url='http://maps.googleapis.com/maps/api/geocode/json?address=%s,%s&sensor=false' % (
                item['en_name'], item['en_country']), callback=self.parse_geocode, meta={'item': item, 'lang': 'zh'},
                          headers={'Accept-Language': 'zh-CN'}, dont_filter=True)

    def parse_geocode(self, response):
        item = response.meta['item']
        lang = response.meta['lang']
        try:
            data = json.loads(response.body)
            if data['status'] == 'OVER_QUERY_LIMIT':
                return Request(url=response.url, callback=self.parse_geocode, meta={'item': item, 'lang': lang},
                               headers={'Accept-Language': response.request.headers['Accept-Language'][0]},
                               dont_filter=True)
            elif data['status'] == 'ZERO_RESULTS':
                return
            elif data['status'] != 'OK':
                self.log('ERROR GEOCODING. STATUS=%s, URL=%s' % (data['status'], response.url))
                return

            city_result = None
            location = None
            for result in data['results']:
                # 必须和原来的经纬度比较接近，才能采信
                geometry = result['geometry']
                lat = geometry['location']['lat']
                lng = geometry['location']['lng']
                dist = utils.haversine(lng, lat, item['lng'], item['lat'])
                if dist > 100:
                    continue
                else:
                    city_result = result
                    location = [lng, lat]
                    break

            if city_result:
                # 查找第一个types包含political的项目
                address_components = filter(lambda val: 'political' in val['types'], city_result['address_components'])
                data = address_components[0]

                short_name = data['short_name']
                long_name = data['long_name']
                s = set(item['alias'])
                s.add(short_name.lower())
                s.add(long_name.lower())
                k = 'zh_name' if lang == 'zh' else 'en_name'
                s.add(item[k].lower())
                item[k] = long_name
                item['alias'] = list(s)
                if location:
                    item['lng'] = location[0]
                    item['lat'] = location[1]

        except (KeyError, IndexError):
            self.log('ERROR GEOCODEING: %s' % response.url, log.WARNING)

        if lang == 'zh':
            return Request(url='http://maps.googleapis.com/maps/api/geocode/json?address=%s,%s&sensor=false' % (
                item['en_name'], item['en_country']), callback=self.parse_geocode, meta={'item': item, 'lang': 'en'},
                           headers={'Accept-Language': 'en-US'}, dont_filter=True)
        else:
            return item


class GeoNamesProcPipeline(object):
    """
    处理GeoNames的城市数据
    """

    spiders = [GeoNamesProcSpider.name]

    country_map = {}

    def process_item(self, item, spider):
        col_loc = get_mongodb('geo', 'Locality', profile='mongodb-general')

        # get country
        country_code = item['country_code']
        if country_code not in GeoNamesProcPipeline.country_map:
            col_country = get_mongodb('geo', 'Country', profile='mongodb-general')
            country = col_country.find_one({'code': country_code})
            assert country != None
            GeoNamesProcPipeline.country_map[country_code] = country
        else:
            country = GeoNamesProcPipeline.country_map[country_code]

        city_id = item['city_id']
        city = col_loc.find_one({'source.geonames.id': city_id})

        if not city:
            city = col_loc.find_one({'alias': item['en_name'].lower(),
                                     'location': {
                                         '$near': {'type': 'Point', 'coordinates': [item['lng'], item['lat']]}},
                                     'country._id': country['_id']})
            if city:
                dist = utils.haversine(city['location']['coordinates'][0], city['location']['coordinates'][1],
                                       item['lng'], item['lat'])
                if dist > 100:
                    city = {}

        if not city:
            city = {}

        city['enName'] = item['en_name']
        zh_name = item['zh_name']
        short_name = utils.get_short_loc(zh_name)
        city['zhName'] = short_name

        alias1 = city['alias'] if 'alias' in city and city['alias'] else []
        alias2 = item['alias'] if 'alias' in item and item['alias'] else []
        alias1.extend(alias2)
        alias1.append(short_name.lower())
        city['alias'] = list(set(filter(lambda val: val, [tmp.lower().strip() for tmp in alias1])))

        source = city['source'] if 'source' in city else {}
        source['geonames'] = {'id': item['city_id']}
        city['source'] = source
        city['country'] = {'id': country['_id'], '_id': country['_id']}
        for k in ('enName', 'zhName'):
            if k in country:
                city['country'][k] = country[k]

        city['level'] = 2 if item['level'] == 'PPLA' else 3
        city['images'] = []
        city['location'] = {'type': 'Point', 'coordinates': [item['lng'], item['lat']]}
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

        col = get_mongodb('raw_data', 'TravelGisCity', 'localhost', 27027)

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

        col = get_mongodb('raw_data', 'GeoNamesCity', 'localhost', 27027)

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


class QyerCityProcSpider(CrawlSpider):
    """
    处理穷游的城市数据
    """

    name = 'qyer-city-proc'

    country_map = {}
    missed_countries = set([])

    def __init__(self, *a, **kw):
        super(QyerCityProcSpider, self).__init__(*a, **kw)

    def start_requests(self):
        param = getattr(self, 'param', {})
        if 'country' not in param:
            param['country'] = []
        yield Request(url='http://www.baidu.com', meta={'country': param['country']}, callback=self.parse)

    def parse(self, response):
        col = get_mongodb('raw_data', 'QyerCity', profile='mongodb-crawler')
        countries = response.meta['country']

        query = {}
        if countries:
            if len(countries) > 1:
                query['$or'] = [{'cname': tmp} for tmp in countries]
            else:
                query['cname'] = countries[0]
        for entry in col.find(query):
            city = entry

            item = CityItem()
            item['city_id'] = int(city['id'])
            item['en_name'] = city['code']
            item['zh_name'] = city['name']

            s = set([tmp.lower().strip() for tmp in (item['alias'] if 'alias' in city else [])])
            s.add(city['code'].lower())
            s.add(city['name'].lower())
            item['alias'] = list(s)

            country_name = city['cname']
            if country_name in QyerCityProcSpider.country_map:
                country = QyerCityProcSpider.country_map[country_name]
            elif country_name not in QyerCityProcSpider.missed_countries:
                col_country = get_mongodb('geo', 'Country', profile='mongodb-general')
                country = col_country.find_one({'alias': country_name})
                if not country:
                    self.log('MISSED COUNTRY: %s' % country_name, log.WARNING)
                    QyerCityProcSpider.missed_countries.add(country_name)
                    continue
                else:
                    QyerCityProcSpider.country_map[country_name] = country
            else:
                continue

            item['en_country'] = country['enName'] if 'enName' in country else None
            item['zh_country'] = country['zhName'] if 'zhName' in country else None
            item['country_code'] = country['code']

            desc = city['ctyprofile_intro'] if 'ctyprofile_intro' in city else ''
            if desc:
                sel = Selector(text=desc)
                item['desc'] = '\n'.join(
                    filter(lambda val: val, [tmp.strip() for tmp in sel.xpath('//p/text()').extract()]))
            else:
                item['desc'] = ''

            img_list = city['img']
            if not img_list:
                img_list = ''

            def _image_proc(url):
                m = re.search(r'^(.+pic\.qyer\.com/album/.+/index)/[0-9x]+$', url)
                return m.group(1) if m else url

            item['imageList'] = map(_image_proc, filter(lambda val: val, [tmp.strip() for tmp in img_list.split(',')]))

            item['url'] = city['url']
            item['is_hot'] = city['is_hot']

            yield Request(url='http://maps.googleapis.com/maps/api/geocode/json?address=%s,%s&sensor=false' % (
                item['en_name'], item['en_country']), callback=self.parse_geocode, meta={'item': item, 'lang': 'zh'},
                          headers={'Accept-Language': 'zh-CN'}, dont_filter=True)

    def parse_geocode(self, response):
        item = response.meta['item']
        lang = response.meta['lang']
        try:
            data = json.loads(response.body)
            if data['status'] == 'OVER_QUERY_LIMIT':
                return Request(url=response.url, callback=self.parse_geocode, meta={'item': item, 'lang': lang},
                               headers={'Accept-Language': response.request.headers['Accept-Language'][0]},
                               dont_filter=True)
            elif data['status'] == 'ZERO_RESULTS':
                return
            elif data['status'] != 'OK':
                self.log('ERROR GEOCODING. STATUS=%s, URL=%s' % (data['status'], response.url))
                return

            geometry = data['results'][0]['geometry']
            # 查找第一个types包含political的项目
            address_components = filter(lambda val: 'political' in val['types'],
                                        data['results'][0]['address_components'])
            data = address_components[0]

            short_name = data['short_name']
            long_name = data['long_name']
            s = set(item['alias'])
            s.add(short_name.lower())
            s.add(long_name.lower())
            k = 'zh_name' if lang == 'zh' else 'en_name'
            s.add(item[k])
            item[k] = long_name
            item['alias'] = list(s)
            item['lat'] = geometry['location']['lat']
            item['lng'] = geometry['location']['lng']
        except (KeyError, IndexError):
            self.log('ERROR GEOCODEING: %s' % response.url, log.WARNING)

        if lang == 'zh':
            return Request(url='http://maps.googleapis.com/maps/api/geocode/json?address=%s,%s&sensor=false' % (
                item['en_name'], item['en_country']), callback=self.parse_geocode, meta={'item': item, 'lang': 'en'},
                           headers={'Accept-Language': 'en-US'}, dont_filter=True)
        else:
            return item


class QyerCityProcPipeline(object):
    """
    处理穷游的城市数据
    """

    spiders = [QyerCityProcSpider.name]

    country_map = {}

    def process_item(self, item, spider):
        col_loc = get_mongodb('geo', 'Locality', profile='mongodb-general')

        # get country
        country_code = item['country_code']
        if country_code not in QyerCityProcPipeline.country_map:
            col_country = get_mongodb('geo', 'Country', profile='mongodb-general')
            country = col_country.find_one({'code': country_code})
            assert country != None
            QyerCityProcPipeline.country_map[country_code] = country
        else:
            country = QyerCityProcPipeline.country_map[country_code]

        city_id = item['city_id']
        city = col_loc.find_one({'source.qyer.id': city_id})

        if not city:
            city = col_loc.find_one({'alias': item['zh_name'].lower(),
                                     'location': {
                                         '$near': {'type': 'Point', 'coordinates': [item['lng'], item['lat']]}},
                                     'country._id': country['_id']})
            if city:
                dist = utils.haversine(city['location']['coordinates'][0], city['location']['coordinates'][1],
                                       item['lng'], item['lat'])
                if dist > 100:
                    city = {}

        if not city:
            city = {}

        city['enName'] = item['en_name']
        zh_name = item['zh_name']
        short_name = utils.get_short_loc(zh_name)
        city['zhName'] = short_name

        alias1 = city['alias'] if 'alias' in city and city['alias'] else []
        alias2 = item['alias'] if 'alias' in item and item['alias'] else []
        alias1.extend(alias2)
        alias1.append(short_name)
        city['alias'] = list(set(filter(lambda val: val, [tmp.lower().strip() for tmp in alias1])))

        source = city['source'] if 'source' in city else {}
        source['qyer'] = {'id': item['city_id'], 'url': item['url']}
        city['source'] = source
        city['country'] = {'id': country['_id'], '_id': country['_id']}
        for k in ('enName', 'zhName'):
            if k in country:
                city['country'][k] = country[k]

        city['level'] = 2
        city['desc'] = item['desc']
        city['imageList'] = item['imageList']
        city['images'] = []
        city['location'] = {'type': 'Point', 'coordinates': [item['lng'], item['lat']]}
        city['abroad'] = country_code != 'CN'
        city['isHot'] = item['is_hot'] > 0

        col_loc.save(city)

        return item