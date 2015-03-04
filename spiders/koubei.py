# coding=utf-8
import json

from scrapy import Item, Field, Request

from spiders import AizouCrawlSpider, AizouPipeline
from utils.database import get_mongodb


__author__ = 'zephyre'


class KoubeiItem(Item):
    # 包括：locality, attraction, dining, shopping
    type = Field()
    data = Field()


class KoubeiSpider(AizouCrawlSpider):
    """
    抓取口碑旅行的数据
    """

    name = 'koubei'
    uuid = 'c9835f1c-bb3d-4501-90ec-fb3b3c767dd9'

    def __init__(self, *args, **kwargs):
        AizouCrawlSpider.__init__(self, *args, **kwargs)

    def start_requests(self):
        # 获取国家列表
        yield Request(url='http://www.koubeilvxing.com/countrys', callback=self.parse_country)

    def parse_country(self, response):
        data = json.loads(response.body)

        for continent in data['continents']:
            for country in continent['countrys']:
                cid = int(country['id'])
                page = 1
                page_size = 50
                tmpl = 'http://www.koubeilvxing.com/places?countryId=%d&page=%d&rows=%d'
                meta = {'page': page, 'countryId': cid, 'pageSize': page_size, 'tmpl': tmpl,
                        'country_en': country['name'], 'country_cn': country['name_cn']}
                url = tmpl % (cid, page, page_size)
                yield Request(url=url, callback=self.parse_cities, meta={'data': meta})

    def parse_cities(self, response):
        data = json.loads(response.body)

        meta = response.meta['data']
        cid = meta['countryId']
        page = meta['page']
        page_size = meta['pageSize']
        tmpl = meta['tmpl']
        country_en = meta['country_en']
        country_cn = meta['country_cn']

        ret_list = data['places'] if 'places' in data else []

        # 确定是否需要翻页
        if len(ret_list) >= page_size:
            page += 1
            meta = {'page': page, 'countryId': cid, 'pageSize': page_size, 'tmpl': tmpl,
                    'country_en': country_en, 'country_cn': country_cn}
            url = tmpl % (cid, page, page_size)
            yield Request(url=url, callback=self.parse_cities, meta={'data': meta})

        for city in ret_list:
            for k in ['id', 'continentId', 'countryId', 'hotelCount', 'restaurantCount', 'attractionCount',
                      'activityCount', 'shoppingCount', 'hotelReviewCount', 'restaurantReviewCount',
                      'attractionReviewCount', 'activityReviewCount', 'shoppingReviewCount', 'coverPhotoId']:
                try:
                    city[k] = int(city[k])
                except ValueError:
                    city[k] = None

            for k in ['lat', 'lng']:
                try:
                    city[k] = float(city[k])
                except ValueError:
                    city[k] = None

            photo_list = city['photoIds']
            if photo_list:
                photo_list = [int(tmp) for tmp in photo_list.split(',')]
            else:
                photo_list = []
            city['photoIds'] = photo_list

            city['enCountry'] = country_en
            city['zhCountry'] = country_cn

            item = KoubeiItem()
            item['type'] = 'locality'
            item['data'] = city
            yield item

            # 搜索该城市的POI
            tmpl = 'http://www.koubeilvxing.com/search?module=%s&new=1&page=%d&placeId=%d&rows=%d'
            city_id = city['id']
            page_size = 100

            for poi_type in ['attraction', 'restaurant', 'hotel', 'activity', 'shopping']:
                page = 1
                meta = {'page': page, 'pageSize': page_size, 'tmpl': tmpl, 'cityId': city_id, 'type': poi_type}
                url = tmpl % (poi_type, page, city_id, page_size)
                yield Request(url=url, callback=self.parse_poi_list, meta={'data': meta})

    def parse_poi_list(self, response):
        data = json.loads(response.body)

        meta = response.meta['data']
        page = meta['page']
        page_size = meta['pageSize']
        tmpl = meta['tmpl']
        city_id = meta['cityId']
        poi_type = meta['type']

        ret_list = data['list'] if 'list' in data else []

        # 确定是否需要翻页
        if len(ret_list) >= page_size:
            page += 1

            meta = {'page': page, 'pageSize': page_size, 'tmpl': tmpl, 'cityId': city_id, 'type': poi_type}
            url = tmpl % (poi_type, page, city_id, page_size)
            yield Request(url=url, callback=self.parse_poi_list, meta={'data': meta})

        for poi in ret_list:
            yield Request(url='http://www.koubeilvxing.com/iteminfo?module=%s&recordId=%d' % (poi_type, int(poi['id'])),
                          callback=self.parse_poi, meta={'data': {'type': poi_type}})

    @staticmethod
    def parse_poi(response):
        poi = json.loads(response.body)['item']
        poi_type = response.meta['data']['type']

        for k in ['id', 'countryId', 'cityId', 'coverPhotoId', 'reviewCount', 'positiveReviewCount',
                  'neutralReviewCount', 'negativeReviewCount']:
            try:
                poi[k] = int(poi[k])
            except (ValueError, KeyError):
                poi[k] = None

        for k in ['lat', 'lng', 'price', 'score']:
            try:
                poi[k] = float(poi[k])
            except (KeyError, ValueError):
                poi[k] = None

        item = KoubeiItem()
        item['type'] = poi_type
        item['data'] = poi
        yield item


class KoubeiPipeline(AizouPipeline):
    """
    口碑数据进入原始库
    """

    spiders = [KoubeiSpider.name]
    spiders_uuid = [KoubeiSpider.uuid]

    def process_item(self, item, spider):
        if not self.is_handler(item, spider):
            return item

        data = item['data']
        poi_type = item['type']

        col_names = {'locality': 'Locality', 'attraction': 'Viewspot', 'restaurant': 'Dining', 'shopping': 'Shopping',
                     'activity': 'Activity', 'hotel': 'Hotel'}
        col = get_mongodb('raw_koubei', col_names[poi_type], 'mongo-raw')

        col.update({'id': data['id']}, {'$set': data}, upsert=True)

        return item

