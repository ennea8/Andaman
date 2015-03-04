# coding: utf-8
import urlparse

import scrapy

from spiders import AizouCrawlSpider, AizouPipeline
import utils
from utils.database import get_mongodb


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
        col = get_mongodb('raw_data', 'QyerSpot', profile='mongodb-crawler')
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

    def start_requests(self):
        param = getattr(self, 'param', {})
        countries = param['country'] if 'country' in param else []
        yield Request(url='http://www.baidu.com', meta={'countries': countries}, callback=self.parse)

    def parse(self, response):
        meta = response.meta
        col_raw = get_mongodb('raw_data', 'QyerSpot', profile='mongodb-crawler')

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

                # 这一步是为了获得poi所在城市
                url = 'http://maps.googleapis.com/maps/api/geocode/json?address=%f,%f' % (lat, lng)
                yield Request(url=url, meta={'item': item}, callback=self.parse_geocode, dont_filter=True)

    def parse_geocode(self, response):
        item = response.meta['item']

        data = json.loads(response.body)
        if data['status'] == 'OVER_QUERY_LIMIT':
            return Request(url=response.url, callback=self.parse_geocode, meta={'item': item}, dont_filter=True)
        elif data['status'] == 'ZERO_RESULTS':
            return
        elif data['status'] != 'OK':
            self.log('ERROR GEOCODING. STATUS=%s, URL=%s' % (data['status'], response.url))
            return

        address_components = data['results'][0]['address_components']
        country_name = filter(lambda val: 'country' in val['types'], address_components)[0]['long_name']
        item['country_info'] = country_name
        item = self.update_country(item)
        if not item:
            return

        item['poi_city'] = map(lambda val: val['long_name'],
                               filter(lambda val: 'political' in val['types'] and 'country' not in val['types'],
                                      address_components))
        item = self.update_city(item)
        if not item:
            return

        url = 'https://maps.googleapis.com/maps/api/geocode/json?address=%s,%s,%s' % (
            item['poi_englishName'], item['poi_city']['enName'], item['country_info']['enName'])
        item['alias'] = []
        return Request(url=url, headers={'Accept-Language': 'en-US'}, meta={'item': item, 'lang': 'en'},
                       callback=self.parse_alias, dont_filter=True)

    def parse_alias(self, response):
        item = response.meta['item']
        lang = response.meta['lang']

        data = json.loads(response.body)
        if data['status'] == 'OVER_QUERY_LIMIT':
            return Request(url=response.url, callback=self.parse_alias, meta={'item': item, 'lang': lang},
                           dont_filter=True)
        elif data['status'] == 'ZERO_RESULTS':
            return
        elif data['status'] != 'OK':
            self.log('ERROR GEOCODING. STATUS=%s, URL=%s' % (data['status'], response.url))
            return

        c = data['results'][0]['address_components'][0]
        # 找到的是行政区还是POI？
        if 'political' not in c['types']:
            alias = set(item['alias'])
            alias.add(c['long_name'].lower())
            alias.add(c['short_name'].lower())
            item['alias'] = list(alias)

            # 顺便处理viewport
            if 'viewport' not in item:
                viewport = data['results'][0]['geometry']['viewport']
                lat = item['poi_lat']
                lng = item['poi_lng']
                if lat >= viewport['southwest']['lat'] and lat <= viewport['northeast']['lat'] and lng >= \
                        viewport['southwest']['lng'] and lng <= viewport['northeast']['lng']:
                    item['viewport'] = viewport

        if lang == 'zh':
            return item
        else:
            url = 'https://maps.googleapis.com/maps/api/geocode/json?address=%s,%s,%s' % (
                item['poi_name'], item['poi_city']['enName'], item['country_info']['enName'])
            return Request(url=url, headers={'Accept-Language': 'zh-CN'}, meta={'item': item, 'lang': 'zh'},
                           callback=self.parse_alias, dont_filter=True)

    def update_country(self, item):
        country_name = item['country_info']
        # lookup the country
        col_country = get_mongodb('geo', 'Country', profile='mongodb-general')
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
        col_loc = get_mongodb('geo', 'Locality', profile='mongodb-general')
        for city_name in city_candidates:
            city_list = list(col_loc.find({'country.id': country_info['_id'],
                                           'alias': re.compile(r'^%s' % city_name.lower()),
                                           'location': {
                                               '$near': {
                                                   '$geometry': {'type': 'Point',
                                                                 'coordinates': [item['poi_lng'], item['poi_lat']]},
                                                   '$minDistance': 0,
                                                   '$maxDistance': 100 * 1000
                                               }
                                           }},
                                          {'zhName': 1, 'enName': 1, 'coords': 1}).limit(5))
            if city_list:
                city = city_list[0]
                break

        if not city:
            self.log('Failed to find locality from DB: %s' % ', '.join(city_candidates), log.WARNING)
            return

        alias_names = list(set(filter(lambda val: val, [(city[k].strip() if k in city and city[k] else '') for k in
                                                        ['zhName', 'enName']])))
        try:
            zhName = city['zhName'].strip()
        except (ValueError, KeyError, AttributeError):
            zhName = alias_names[0]
        try:
            enName = city['enName'].strip()
        except (ValueError, KeyError, AttributeError):
            enName = alias_names[0]
        item['poi_city'] = {'id': city['_id'], '_id': city['_id'], 'zhName': zhName, 'enName': enName}
        return item


class QyerSpotProcPipeline(object):
    spiders = [QyerVsProcSpider.name]

    def process_item(self, item, spider):
        city_info = item['poi_city']
        country_info = item['country_info']

        # lookup the poi
        col_vs = get_mongodb('poi', 'ViewSpot', profile='mongodb-general')
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

        def _image_proc(url):
            m = re.search(r'^(.+pic\.qyer\.com/album/.+/index)/[0-9x]+$', url)
            return m.group(1) if m else url

        vs['imageList'] = map(_image_proc, item['poi_photo'] if 'poi_photo' in item and item['poi_photo'] else [])

        vs['country'] = country_info
        vs['city'] = city_info

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
                vs['address'] = entry['content']
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


class QyerMddItem(scrapy.Item):
    data = scrapy.Field()
    type = scrapy.Field()


class QyerBaseSpider(AizouCrawlSpider):
    """
    穷游基类。通过http://place.qyer.com/，遍历各个国家
    """

    def __init__(self, *a, **kw):
        super(QyerBaseSpider, self).__init__(*a, **kw)
        self.country_filter = None
        self.cont_map = {
            'as': 'Asialist',
            'eu': 'Europelist',
            'af': 'Africalist',
            'na': 'NorthAmericalist',
            'sa': 'SouthAmericalist',
            'oc': 'Oceanialist',
            'an': 'Antarcticalist'
        }

    def start_requests(self):
        yield Request(url='http://place.qyer.com')

    def parse_country_helper(self, response):
        if 'cont' in self.param:
            self.cont_map = {tmp: self.cont_map[tmp] for tmp in self.param['cont']}

        if 'country' in self.param:
            self.country_filter = [int(tmp) for tmp in self.param['country']]

        sel = Selector(response)

        for cont in self.cont_map:
            cont_node = sel.xpath('//div[@class="pla_indcountrylists"]/div[@id="%s"]' % self.cont_map[cont])[0]
            for region_node in cont_node.xpath('.//li[@class="item"]'):
                is_hot = bool(region_node.xpath('./p[@class="hot"]').extract())
                tmp = region_node.xpath('.//a[@href and @data-bn-ipg]')
                if not tmp:
                    continue
                region_node = tmp[0]
                zh_name = region_node.xpath('./text()').extract()[0].strip()
                en_name = region_node.xpath('./span[@class="en"]/text()').extract()[0].strip()
                tmp = region_node.xpath('./@data-bn-ipg').extract()[0]
                pid = int(re.search(r'place-index-countrylist-(\d+)', tmp).group(1))
                href = region_node.xpath('./@href').extract()[0]
                url = self.build_href(response.url, href)

                if self.country_filter and pid not in self.country_filter:
                    continue

                item = {'type': 'country'}
                data = {'zhName': zh_name, 'enName': en_name, 'alias': {zh_name.lower(), en_name.lower()},
                        'isHot': is_hot, 'id': pid, 'url': url}
                item['data'] = data

                yield item


class QyerMddSpider(QyerBaseSpider):
    """
    穷游目的地的抓取
    """

    name = 'qyer-mdd'
    uuid = 'f948c479-217d-49f5-b226-81e58ddef99c'

    def parse(self, response):
        for result in self.parse_country_helper(response):
            item = QyerMddItem()
            item['type'] = result['type']
            item['data'] = result['data']

            yield item


class QyerNoteSpider(QyerBaseSpider):
    """
    穷游游记的抓取
    """
    name = 'qyer-note'
    uuid = '8b8134ff-6416-49d4-bd10-a1aaa0a38cf6'

    def parse(self, response):
        from urlparse import urljoin

        for result in self.parse_country_helper(response):
            meta = {'country_id': result['data']['id']}
            url = result['data']['url']
            if url[-1] != '/':
                url += '/'
            url = urljoin(url, 'travel-notes/page1')

            yield Request(url=url, meta={'data': meta}, callback=self.parse_note_list)

    def get_author(self, user_node):
        from urlparse import urlparse, urlunparse

        user_id = int(re.search(r'qyer\.com/u/(\d+)', user_node.xpath('./a[@href]/@href').extract()[0]).group(1))
        avatar = user_node.xpath(r'./a[@href]/img[@src and @alt]/@src').extract()[0]
        avatar = re.sub(r'_avatar_(small|middle)\.jpg', '_avatar_big.jpg', avatar)
        tmp = urlparse(avatar)
        avatar = urlunparse((tmp.scheme, tmp.netloc, tmp.path, '', '', ''))
        user_name = user_node.xpath(r'./a[@href]/img[@src and @alt]/@alt').extract()[0].strip()

        return {'user_id': user_id, 'avatar': avatar, 'user_name': user_name}

    def parse_note_list(self, response):
        sel = Selector(response)
        for entry_node in sel.xpath(
                '//div[@class="pla_main"]/ul[contains(@class,"pla_travellist")]/li[@class="item"]'):
            tmp = entry_node.xpath('./div[@class="pic"]//img[@src]/@src').extract()
            cover_url = self.build_href(response.url, tmp[0]) if tmp else None
            if cover_url == 'http://static.qyer.com/images/place/no/poi_200_133.png':
                cover_url = None

            header = entry_node.xpath('./div[@class="cnt"]/div[@class="top"]')[0]

            user_node = header.xpath('./p[@class="face"]')[0]
            author = self.get_author(user_node)

            title_node = header.xpath('./*[@class="title"]/a[@href]')[0]
            title = title_node.xpath('./text()').extract()[0].strip()
            url = title_node.xpath('./@href').extract()[0]
            note_id = int(re.search(r'thread-(\d+)-\d+\.html', url).group(1))

            ctime = header.xpath('.//p[@class="fr"]/span[@class="time"]/text()').extract()[0]
            view_cnt = int(header.xpath('.//p[@class="fr"]/span[@class="bbsview"]/text()').extract()[0])
            comment_cnt = int(header.xpath('.//p[@class="fr"]/span[@class="bbsreply"]/text()').extract()[0])
            favor_cnt = int(header.xpath('.//p[@class="fr"]/span[@class="bbslike"]/text()').extract()[0])

            meta = copy.deepcopy(response.meta['data'])
            meta['cover_url'] = cover_url
            meta['user_id'] = author['user_id']
            meta['avatar'] = author['avatar']
            meta['user_name'] = author['user_name']
            meta['title'] = title
            meta['note_id'] = note_id
            meta['ctime'] = ctime
            meta['view_cnt'] = view_cnt
            meta['comment_cnt'] = comment_cnt
            meta['favor_cnt'] = favor_cnt

            # self.log('%s' % meta, log.INFO)
            self.log('user: %s, title: %s, tid: %d' % (meta['user_name'], meta['title'], meta['note_id']), log.INFO)

            yield Request(url='http://bbs.qyer.com/viewthread.php?tid=%d&page=1' % meta['note_id'],
                          meta={'data': meta}, callback=self.parse_post)

        tmp = sel.xpath('//div[@class="ui_page"]/a[@href and contains(@class,"ui_page_next")]/@href').extract()
        if tmp:
            next_page = self.build_href(response.url, tmp[0])
            yield Request(url=next_page, meta={'data': response.meta['data']}, callback=self.parse_note_list)

    def parse_post(self, response):
        sel = Selector(response)

        for post_node in sel.xpath('//div[@id="postlist"]/div[@id and @class="bbs_postview"]'):
            post_id = int(re.search(r'post_(\d+)', post_node.xpath('./@id').extract()[0]).group(1))
            post_contents = post_node.xpath(
                './div[contains(@class,"bbs_txttop") or contains(@class,"bbs_txtbox")]').extract()
            data = copy.deepcopy(response.meta['data'])
            data['post_id'] = post_id
            data['post_contents'] = post_contents

            item = QyerMddItem()
            item['type'] = 'note'
            item['data'] = data

            yield item

        tmp = sel.xpath(
            '//div[contains(@class,"forumcontrol")]/div[@class="pages"]/a[@href and @data-bn-ipg and @class="next"]/@href').extract()
        if tmp:
            yield Request(url=self.build_href(response.url, tmp[0]), meta={'data': response.meta['data']},
                          callback=self.parse_post)


class QyerNotePipeline(AizouPipeline):
    spiders = [QyerNoteSpider.name]

    spiders_uuid = [QyerNoteSpider.uuid]

    def process_item(self, item, spider):
        if not self.is_handler(item, spider):
            return item

        data = item['data']
        col = self.fetch_db_col('raw_data', 'QyerNote', 'mongodb-crawler')

        col.update({'note_id': data['note_id'], 'post_id': data['post_id']}, {'$set': data}, upsert=True)

        return item


class QyerMddPipeline(AizouPipeline):
    spiders = [QyerMddSpider.name]

    spiders_uuid = [QyerMddSpider.uuid]

    def process_item(self, item, spider):
        if not self.is_handler(item, spider):
            return item

        if item['type'] == 'country':
            return self.process_country(item, spider)

        return item

    def process_country(self, item, spider):
        col = self.fetch_db_col('geo', 'Country', 'mongodb-general')

        data = item['data']
        alias_list = list(data['alias'])
        ret = col.find_and_modify({'alias': {'$in': alias_list}},
                                  {'$addToSet': {'alias': {'$each': alias_list}},
                                   '$set': {'source.qyer': {'id': data['id'], 'url': data['url']},
                                            'zhName': data['zhName'],
                                            'enName': data['enName']}})

        if ret:
            spider.log('%s => %s' % (item['data']['zhName'], ret['zhName']), log.INFO)
        else:
            spider.log('Cannot find: %s' % item['data']['zhName'], log.INFO)

        return item







