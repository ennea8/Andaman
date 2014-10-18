# coding=utf-8
import json

import MySQLdb
from MySQLdb.cursors import DictCursor
import pymongo
from scrapy import Request, Selector
import scrapy
from scrapy.contrib.spiders import CrawlSpider

from items import QunarPoiItem


__author__ = 'zephyre'


class QunarPoiImageItem(scrapy.Item):
    # define the fields for your item here like:
    image_list = scrapy.Field()
    vs_id = scrapy.Field()


class QunarPoiImagePipeline(object):
    def process_item(self, item, spider):
        if not isinstance(item, QunarPoiImageItem):
            return item

        col = pymongo.Connection().Poi.ViewSpot
        vs = col.find_one({'_id': item['vs_id']})

        if not self.db:
            self.connect()

        data = item['data']
        ret = self.db.Poi.find_one({'id': data['id']}, {'id': 1})
        if not ret:
            self.db.Poi.insert(data)

        return item


class QunarImageSpider(CrawlSpider):
    """
    去哪儿的POI数据抓下来以后，存放在QunarRawPoi的数据库里面。
    根据这些数据，访问相应的原始网站，抓取其中的图像。
    """

    def __init__(self, *a, **kw):
        self.name = 'qunar_image'
        super(QunarImageSpider, self).__init__(*a, **kw)

    def start_requests(self):
        col = pymongo.Connection().poi.ViewSpot

        results = list(col.find({'source.qunar.url': {'$ne': None}}, {'imageList'}))

        for idx, item in enumerate(results):
            for img in item['imageList']:
                yield Request(url=img, callback=self.parse, meta={'data': {'_id': item['_id']}})

    def parse(self, response):
        sel = Selector(response)
        img_list = []
        for item in sel.xpath(
                '//div[@class="originalbox"]/ul[@id="idSlider"]/img[@data-beacon="poi_pic"]/@src').extract():
            img_list.append(item)


class QunarPoiSpider(CrawlSpider):
    def __init__(self, poi_type, *a, **kw):
        self.name = 'qunar_poi'
        self.poi_type = poi_type
        # id的查询范围
        self.id_range = kw['idRange'] if 'idRange' in kw else None

        super(QunarPoiSpider, self).__init__(*a, **kw)

    def start_requests(self):
        conn = MySQLdb.connect(host='localhost', port=3306, user='root', passwd='07996019Zh', db='vxp_raw',
                               cursorclass=DictCursor, charset='utf8')
        cursor = conn.cursor()
        if self.id_range:
            stmt = 'SELECT DISTINCT id FROM vxp_city ORDER BY id LIMIT %d, %d' % (self.id_range[0], self.id_range[1])
        else:
            stmt = 'SELECT DISTINCT id FROM vxp_city ORDER BY id'
        cursor.execute(stmt)
        for row in cursor:
            url_template = 'http://travel.qunar.com/place/api/city/poi?cityId=%d&type=%d&offset=%d&limit=%d'
            m = {'cityId': row['id'], 'offset': 0, 'limit': 50, 'poiType': self.poi_type, 'urlTemplate': url_template}
            url = url_template % (m['cityId'], m['poiType'], m['offset'], m['limit'])

            yield Request(url=url, callback=self.parse, meta={'data': m})

    def parse(self, response):
        try:
            data = json.loads(response.body)
            parse_suc = True
        except ValueError:
            parse_suc = False

        if parse_suc:
            for entry in data['data']:
                item = QunarPoiItem()
                item['data'] = entry
                yield item

            tot = data['totalCount']
            user_data = response.meta['data']
            offset = user_data['offset']
            limit = user_data['limit']
            city_id = user_data['cityId']
            template = user_data['urlTemplate']
            poi_type = user_data['poiType']

            offset += limit
            if offset < tot:
                url = template % (city_id, poi_type, offset, limit)
                user_data['offset'] = offset
                yield Request(url=url, callback=self.parse, meta={'data': user_data})
        else:
            yield response.request
