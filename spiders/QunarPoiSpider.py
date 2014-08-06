# coding=utf-8
import json

import MySQLdb
from MySQLdb.cursors import DictCursor
from scrapy import Request
from scrapy.contrib.spiders import CrawlSpider

from items import QunarPoiItem


__author__ = 'zephyre'


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
        data = json.loads(response.body)
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
