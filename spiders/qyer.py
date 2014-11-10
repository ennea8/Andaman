# coding: utf-8
import random
import urlparse

import scrapy

import utils


__author__ = 'zwh'
import copy
import re
import json

from scrapy import Request, Selector, log
from scrapy.contrib.spiders import CrawlSpider


class QyerPoiItem(scrapy.Item):
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
    rating = scrapy.Field()
    commentCnt = scrapy.Field()
    alias = scrapy.Field()
    viewport = scrapy.Field()


class QyerVsSpot(CrawlSpider):
    name = 'qyer-vs'

    def __init__(self, *a, **kw):
        super(QyerVsSpot, self).__init__(*a, **kw)
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

            if 'country' in self.param and country_engname.lower() not in self.param['country']:
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

        # 景点评分
        poi_info['rating'] = None
        tmp = sel.xpath(
            '//div[@class="poiDet-main"]//div[@class="infos"]//p[@class="points"]/span[@class="number"]/text()').extract()
        try:
            poi_info['rating'] = float(tmp[0]) / 10 if tmp else None
        except ValueError:
            pass

        # 用户评论次数
        poi_info['commentCnt'] = None
        tmp = sel.xpath(
            '//div[@class="poiDet-main"]//div[@class="infos"]//p[@class="poiDet-stars"]/span[@class="summery"]/a/text()').extract()
        if tmp:
            m = re.search(r'^\s*(\d+)', tmp[0])
            if m:
                poi_info['commentCnt'] = int(m.group(1))

        # 景点访问次数
        poi_info['poi_been'] = None
        tmp = sel.xpath(
            '//div[@class="poiDet-rightfix"]/div[@class="wrap"]/h2[@class="title"]/span[@class="golden"]/text()').extract()
        try:
            poi_info['poi_been'] = int(tmp[0]) if tmp else None
        except ValueError:
            pass

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
        item = QyerPoiItem()
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
        item['rating'] = poi_info['rating']
        item['commentCnt'] = poi_info['commentCnt']
        yield item


class QyerVsPipeline(object):
    spiders = [QyerVsSpot.name]

    def process_item(self, item, spider):
        col = utils.get_mongodb('raw_data', 'QyerSpot', profile='mongodb-crawler')
        data = col.find_one({'poi_id': item['poi_id']})
        if not data:
            data = {}
        for key in item.keys():
            data[key] = item[key]
        col.save(data)
        return item


class QyerVsProcSpider(CrawlSpider):
    """
    处理穷游的景点数据
    """

    name = 'qyer-vs-proc'

    def __init__(self, *a, **kw):
        super(QyerVsProcSpider, self).__init__(*a, **kw)
        section = 'geocode-keys'
        self.geocode_keys = []
        for option in utils.cfg_options(section):
            self.geocode_keys.append(utils.cfg_entries(section, option))

    def start_requests(self):
        param = getattr(self, 'param', {})
        countries = param['country'] if 'country' in param else []
        yield Request(url='http://www.baidu.com', meta={'countries': countries}, callback=self.parse)

    def parse(self, response):
        meta = response.meta
        col_raw = utils.get_mongodb('raw_data', 'QyerSpot', profile='mongodb-crawler')

        for country in meta['countries']:
            # 查找指定国家的POI
            for entry in col_raw.find({'country_info.country_engname': country}):
                lat = entry['poi_lat']
                lng = entry['poi_lng']

                if not lat or not lng:
                    continue

                item = QyerPoiItem()
                for k in entry:
                    if k in item.fields:
                        item[k] = entry[k]

                ridx = random.randint(0, len(self.geocode_keys) - 1)
                geocode_key = self.geocode_keys[ridx]
                url = 'https://maps.googleapis.com/maps/api/geocode/json?address=%f,%f&key=%s' % (lat, lng, geocode_key)
                yield Request(url=url, meta={'item': item}, callback=self.parse_geocode, dont_filter=True)

    def parse_geocode(self, response):
        geocode_ret = json.loads(response.body)
        item = response.meta['item']

        if geocode_ret['status'] == 'OVER_QUERY_LIMIT':
            self.log('OVER_QUERY_LIMIT', log.WARNING)
            return Request(url=response.url, callback=self.parse_geocode, meta={'item': item}, dont_filter=True)

        components = geocode_ret['results'][0]['address_components']

        # 可能的city候选列表
        city_name = []
        for c in components:
            if 'country' in c['types']:
                continue
            else:
                city_name.append(c['long_name'])

        # find country
        tmp = filter(lambda entry: 'country' in entry['types'], components)
        if not tmp:
            self.log('Failed to find country from Geocode: %s' % response.url, log.WARNING)
            return
        country_name = tmp[0]['long_name']

        item['poi_city'] = city_name
        item['country_info'] = country_name

        item = self.update_country(item)
        if not item:
            return
        item = self.update_city(item)
        if not item:
            return

        ridx = random.randint(0, len(self.geocode_keys) - 1)
        geocode_key = self.geocode_keys[ridx]
        url = 'https://maps.googleapis.com/maps/api/geocode/json?address=%s,%s,%s&key=%s' % (
            item['poi_englishName'], item['poi_city']['enName'], item['country_info']['enName'], geocode_key)
        item['alias'] = []
        return Request(url=url, headers={'Accept-Language': 'en-US'}, meta={'item': item, 'lang': 'en'},
                       callback=self.parse_alias, dont_filter=True)

    def parse_alias(self, response):
        geocode_ret = json.loads(response.body)
        item = response.meta['item']
        lang = response.meta['lang']

        if geocode_ret['status'] == 'OVER_QUERY_LIMIT':
            self.log('OVER_QUERY_LIMIT', log.WARNING)
            return Request(url=response.url, headers=response.headers,
                           callback=self.parse_alias, meta={'item': item, 'lang': lang}, dont_filter=True)
        elif geocode_ret['status'] == 'OK':
            c = geocode_ret['results'][0]['address_components'][0]
            # 找到的是行政区还是POI？
            if 'political' not in c['types']:
                alias = set(item['alias'])
                alias.add(c['long_name'].lower())
                alias.add(c['short_name'].lower())
                item['alias'] = list(alias)

                # 顺便处理viewport
                if 'viewport' not in item:
                    viewport = geocode_ret['results'][0]['geometry']['viewport']
                    lat = item['poi_lat']
                    lng = item['poi_lng']
                    if lat >= viewport['southwest']['lat'] and lat <= viewport['northeast']['lat'] and lng >= \
                            viewport['southwest']['lng'] and lng <= viewport['northeast']['lng']:
                        item['viewport'] = viewport
        else:
            self.log(geocode_ret['status'], log.WARNING)

        if lang == 'zh':
            return item
        else:
            ridx = random.randint(0, len(self.geocode_keys) - 1)
            geocode_key = self.geocode_keys[ridx]
            url = 'https://maps.googleapis.com/maps/api/geocode/json?address=%s,%s,%s&key=%s' % (
                item['poi_name'], item['poi_city']['enName'], item['country_info']['enName'], geocode_key)
            return Request(url=url, headers={'Accept-Language': 'zh-CN'}, meta={'item': item, 'lang': 'zh'},
                           callback=self.parse_alias, dont_filter=True)

    def update_country(self, item):
        country_name = item['country_info']
        # lookup the country
        col_country = utils.get_mongodb('geo', 'Country', profile='mongodb-general')
        ret = col_country.find_one({'alias': country_name.lower()}, {'zhName': 1, 'enName': 1})
        if not ret:
            self.log('Failed to find country: %s' % country_name, log.WARNING)
            return
        item['country_info'] = {'id': ret['_id'], '_id': ret['_id'], 'zhName': ret['zhName'], 'enName': ret['enName']}
        return item

    def update_city(self, item):
        city_candidates = item['poi_city']
        country_info = item['country_info']
        # lookup the city
        city = None
        col_loc = utils.get_mongodb('geo', 'Locality', profile='mongodb-general')
        for city_name in city_candidates:
            city_list = list(col_loc.find({'country.id': country_info['_id'],
                                           'alias': re.compile(r'^%s' % city_name.lower()),
                                           'location': {
                                               '$near': {
                                                   '$geometry': {'type': 'Point',
                                                                 'coordinates': [item['poi_lng'], item['poi_lat']]},
                                                   '$minDistance': 0,
                                                   '$maxDistance': 150 * 1000
                                               }
                                           }},
                                          {'zhName': 1, 'enName': 1, 'coords': 1}).limit(5))
            if city_list:
                city = city_list[0]
                break

        if not city:
            self.log('Failed to find locality from DB: %s' % ', '.join(city_candidates), log.WARNING)
            return

        item['poi_city'] = {'id': city['_id'], '_id': city['_id'], 'zhName': city['zhName'], 'enName': city['enName']}
        return item


class QyerSpotProcPipeline(object):
    spiders = [QyerVsProcSpider.name]

    def process_item(self, item, spider):
        city_info = item['poi_city']
        country_info = item['country_info']

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
        vs['imageList'] = item['poi_photo'] if 'poi_photo' in item and item['poi_photo'] else []

        vs['addr'] = {'loc': city_info, 'coords': {'lat': item['poi_lat'], 'lng': item['poi_lng']}}
        vs['country'] = country_info

        alias = filter(lambda val: val,
                       list(set([vs[k].strip().lower() if vs[k] else '' for k in ['name', 'zhName', 'enName']])))
        alias.extend(item['alias'])
        vs['alias'] = list(set(alias))
        vs['rating'] = item['rating'] if 'rating' in item else None

        vs['targets'] = [city_info['_id'], country_info['_id']]
        vs['enabled'] = True
        vs['abroad'] = True

        vs['location'] = {'type': 'Point', 'coordinates': [item['poi_lng'], item['poi_lat']]}
        if 'viewport' in item:
            vs['viewport'] = {'northeast': {'type': 'Point',
                                            'coordinates': [item['viewport']['northeast']['lng'],
                                                            item['viewport']['northeast']['lat']]},
                              'southwest': {'type': 'Point',
                                            'coordinates': [item['viewport']['southwest']['lng'],
                                                            item['viewport']['southwest']['lat']]},
            }

        details = item['poi_detail'] if 'poi_detail' in item else []
        new_det = []
        for entry in details:
            if entry['title'][:2] == u'门票':
                vs['priceDesc'] = entry['content']
            elif entry['title'][:4] == u'到达方式':
                vs['trafficInfo'] = entry['content']
            elif entry['title'][:4] == u'开放时间':
                vs['openTime'] = entry['content']
            elif entry['title'][:2] == u'地址':
                vs['addr']['address'] = entry['content']
            elif entry['title'][:2] == u'网址':
                vs['website'] = entry['content']
            elif entry['title'][:4] == u'所属分类':
                tags = set(vs['tags'] if 'tags' in vs else [])
                for t in re.split(ur'[/\|｜\s,]', entry['content']):
                    # for t in re.split(r'[/\|\s,]', entry['content']):
                    t = t.strip()
                    if t:
                        tags.add(t)
                vs['tags'] = list(tags)
            else:
                new_det.append(entry['title'] + entry['content'])

        col_vs.save(vs)

        return item




