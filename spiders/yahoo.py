# encoding=utf-8
import json

import utils


__author__ = 'lxf'

import re

from scrapy import Request, Selector, Item, Field, log
from scrapy.contrib.spiders import CrawlSpider

from utils import get_mongodb


# -----------------------------------define field------------------------
class YahooCityItem(Item):
    country = Field()  # 国家信息
    state = Field()  # 州/省份
    city = Field()  # 城市
    coords = Field()
    woeid = Field()
    abroad = Field()

    # 1: provinces and states; 2: cities; 3: counties
    level = Field()

    en_country = Field()
    zh_country = Field()
    alias = Field()
    zh_name = Field()
    en_name = Field()


# ----------------------------------define spider------------------------------------
class YahooCitySpider(CrawlSpider):
    name = 'yahoo_city'  # define the spider name

    def __init__(self, *a, **kw):
        super(YahooCitySpider, self).__init__(*a, **kw)
        # key: country code, value: set of all the provinces and states within this country
        self.provinces = {}

    # ---------------------------draw the url-----------------------------------------
    def start_requests(self):  # send request
        country_list = []
        if 'param' in dir(self):
            param = getattr(self, 'param', [])
            if 'country' in param:
                country_list = param['country']

        if not country_list:
            country_list = list([tmp['code'] for tmp in get_mongodb('geo', 'Country').find({}, {'code': 1})])

        first_url = 'https://weather.yahoo.com/'
        for country_code in country_list:
            abroad = (country_code.lower() != 'cn')
            temp_url = first_url + country_code
            data = {'countrycode': country_code, 'abroad': abroad}
            yield Request(url=temp_url, callback=self.parse_state_url, meta={'data': data})

    # ------------------------draw the state url-------------------------------------
    def parse_state_url(self, response):  # draw the state
        sel = Selector(response)
        tempcountryname = sel.xpath(
            '//div[@id="MediaWeatherRegion"]/div[@class="hd"]/div[@class="yom-bread"]/text()').extract()
        match = re.search(r'[\w\s]+$', tempcountryname[0])
        if match:
            countryname = match.group().strip()
        else:
            self.log('没有国家名', log.WARNING)
            return

        data_1 = response.meta['data']

        for node in sel.xpath('//div[@id="page1"]/ul/li/a'):
            state_name = node.xpath('./span/text()').extract()[0].strip()
            state_href = node.xpath('./@href').extract()[0]

            yield Request(url='https://weather.yahoo.com' + state_href, callback=self.parse_city,
                          meta={'data': {'data_1': data_1, 'countryname': countryname, 'state': state_name}})

            country_code = data_1['countrycode']

            # Get states and provinces
            item = YahooCityItem()
            item['country'] = country_code
            item['state'] = state_name
            item['level'] = 1
            item['abroad'] = data_1['abroad']
            yield item

    # ------------------------draw the city url-------------------------------------
    def parse_city(self, response):
        sel = Selector(response)
        city_list = sel.xpath('//div[@id="page1"]/ul/li/a/span/text()').extract()  # city list
        city_list_url = sel.xpath('//div[@id="MediaWeatherRegion"]/div[@class="bd"]/'
                                  'div[@class="weather-regions"]/div/ul/li/a/@href').extract()
        data_2 = response.meta['data']
        if city_list:
            for i in range(0, len(city_list)):
                city_url = 'https://weather.yahoo.com' + city_list_url[i]
                data = {'data_2': data_2, 'cityname': city_list[i], 'city_url': city_url}
                yield Request(url=city_url, callback=self.parse_city_info, meta={'data': data})
        else:
            return

    def parse_city_info(self, response):
        city_url = response.meta['data']['city_url']
        match = re.search(r'\d{1,}', city_url)
        if match:
            woeid = int(match.group())
        else:
            woeid = None

        match = re.search(r'Y\.Media\.Location\.SearchAssist\((.+?)\)', response.body)
        if match:
            loc_data = json.loads(match.groups()[0])
            coords = {'lat': float(loc_data['lat']), 'lng': float(loc_data['lon'])}
        else:
            coords = None
        data_3 = response.meta['data']
        country = {'countrycode': data_3['data_2']['data_1']['countrycode'],
                   'countryname': data_3['data_2']['countryname']}
        item = YahooCityItem()
        item['country'] = country
        item['state'] = data_3['data_2']['state']
        item['city'] = data_3['cityname']
        item['abroad'] = data_3['data_2']['data_1']['abroad']
        item['level'] = 2
        if coords:
            item['coords'] = coords
        item['woeid'] = woeid
        return item
        # -----------------------pipeline--------------------------------------------------


class YahooCityPipeline(object):
    # 向pipline注册
    spiders = [YahooCitySpider.name]

    def process_item(self, item, spider):
        col = get_mongodb('raw_data', 'YahooCityInfo', profile='mongodb-crawler')

        # retrieve the locality if it exists
        if 'woeid' in item:
            data = col.find_one({'woeid': item['woeid'], 'level': 2})
        else:
            data = col.find_one({'country': item['country'], 'state': item['state'], 'level': 1})
        if not data:
            data = {}

        for k in ['country', 'state', 'city', 'abroad', 'coords', 'woeid', 'level']:
            if k in item:
                data[k] = item[k]

        col.save(data)

        return item


class YahooCityProcSpider(CrawlSpider):
    """
    处理Yahoo的城市数据
    """

    name = 'yahoo_city_proc'

    country_map = {}
    missed_countries = set([])

    def __init__(self, *a, **kw):
        super(YahooCityProcSpider, self).__init__(*a, **kw)

    def start_requests(self):
        param = getattr(self, 'param', {})
        countries = param['country'] if 'country' in param else []
        level = int(param['level'][0]) if 'level' in param else None
        yield Request(url='http://www.baidu.com', meta={'countries': countries, 'level': level}, callback=self.parse)

    def parse(self, response):
        col = utils.get_mongodb('raw_data', 'YahooCityInfo', profile='mongodb-crawler')
        countries = response.meta['countries']
        level = response.meta['level']
        query = {'$or': [{'country': tmp} for tmp in countries]} if countries else {}
        if level:
            query['level'] = level

        for entry in list(col.find(query, {'_id': 1})):
            city = col.find_one({'_id': entry['_id']})

            item = YahooCityItem()
            for k in ['country', 'state', 'city', 'coords', 'woeid', 'abroad', 'level']:
                if k in city:
                    item[k] = city[k]

            country_code = city['country']
            if country_code not in self.country_map:
                col_country = utils.get_mongodb('geo', 'Country', profile='mongodb-general')
                country_info = col_country.find_one({'code': country_code})
                if not country_info:
                    self.log('Unable to find country: %s' % country_code, log.WARNING)
                    continue
                self.country_map[country_code] = country_info

            country_info = self.country_map[country_code]

            item['country'] = country_info
            item['en_country'] = country_info['enName']
            item['zh_country'] = country_info['zhName']
            if 'city' in city:
                item['en_name'] = city['city']
                item['zh_name'] = city['city']
            else:
                item['en_name'] = city['state']
                item['zh_name'] = city['state']

            item['alias'] = list({item['en_name'].lower()})

            yield Request(url='http://maps.googleapis.com/maps/api/geocode/json?address=%s,%s&sensor=false' % (
                item['en_name'], item['en_country']), callback=self.parse_geocode,
                          meta={'item': item, 'lang': 'zh'}, headers={'Accept-Language': 'zh-CN'}, dont_filter=True)

    def parse_geocode(self, response):
        item = response.meta['item']
        lang = response.meta['lang']
        try:
            data = json.loads(response.body)['results'][0]
            addr = data['address_components'][0]
            short_name = addr['short_name']
            long_name = addr['long_name']
            s = set(item['alias'])
            s.add(short_name.lower())
            s.add(long_name.lower())
            k = 'zh_name' if lang == 'zh' else 'en_name'
            s.add(item[k].lower())
            item[k] = long_name
            item['alias'] = list(s)

            if 'coords' not in item or not item['coords']:
                item['coords'] = data['geometry']['location']

        except (KeyError, IndexError):
            self.log('ERROR GEOCODEING: %s' % response.url, log.WARNING)

        if lang == 'zh':
            yield Request(url='http://maps.googleapis.com/maps/api/geocode/json?address=%s,%s&sensor=false' % (
                item['en_name'], item['en_country']), callback=self.parse_geocode, meta={'item': item, 'lang': 'en'},
                          headers={'Accept-Language': 'en-US'}, dont_filter=True)
        else:
            yield item


class YahooCityProcPipeline(object):
    """
    处理Yahoo的城市数据
    """

    spiders = [YahooCityProcSpider.name]

    country_map = {}

    def process_item(self, item, spider):
        if type(item).__name__ != YahooCityItem.__name__:
            return item

        col_loc = utils.get_mongodb('geo', 'Locality', profile='mongodb-general')
        data = {}

        level = item['level']

        data['zhName'] = item['zh_name']
        data['enName'] = item['en_name']
        abroad = item['abroad']
        data['abroad'] = abroad
        data['shortName'] = item['en_name' if abroad else 'zh_name']
        data['alias'] = list(set(item['alias']))
        data['pinyin'] = []

        country_info = item['country']
        data['country'] = {'id': country_info['_id'], 'zhName': country_info['zhName'],
                           'enName': country_info['enName']}

        data['level'] = level
        data['images'] = []
        if 'coords' in item:
            data['coords'] = item['coords']

        data['source'] = {'name': 'yahoo'}
        if 'woeid' in item:
            data['source']['id'] = item['woeid']

        if level > 1:
            # cities
            prov = col_loc.find_one({'country.id': country_info['_id'], 'alias': item['state'].lower(), level: 1})
            if prov:
                data['superAdm'] = {'id': prov['_id'], 'zhName': prov['zhName'], 'enName': prov['enName']}
            else:
                spider.log('Cannot find province: %s, %s' % (item['state'], item['en_country']))

        if 'woeid' in item:
            entry = col_loc.find_one({'source.name': 'yahoo', 'source.id': item['woeid']})
        else:
            entry = col_loc.find_one({'country.id': country_info['_id'], 'alias': data['enName'].lower()})

        if not entry:
            entry = {}

        key_set = set(data.keys()) - {'alias'}
        for k in key_set:
            entry[k] = data[k]

        if 'alias' not in entry:
            entry['alias'] = []

        entry['alias'] = list(set(entry['alias']).union(data['alias']))

        col_loc.save(entry)

        return item