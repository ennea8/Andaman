import json

import MySQLdb
from MySQLdb.cursors import DictCursor
from scrapy import Request, Selector
from scrapy.contrib.spiders import CrawlSpider

from items import BaiduPoiItem


__author__ = 'zephyre'


class BaiduPoiSpider(CrawlSpider):
    def __init__(self, *a, **kw):
        self.name = 'baidu_poi'
        super(BaiduPoiSpider, self).__init__(*a, **kw)

    def start_requests(self):
        conn = MySQLdb.connect(host='localhost', port=3306, user='root', passwd='07996019Zh', db='vxp_restore_poi',
                               cursorclass=DictCursor, charset='utf8')
        cursor = conn.cursor()
        cursor.execute('SELECT DISTINCT namePy FROM qunar_all_area')
        for row in cursor:
            city = row['namePy']
            idx = city.find('|')
            if idx != -1:
                city = city[:idx]

            url_template = 'http://lvyou.baidu.com/destination/ajax/jingdian?format=ajax&surl=%s&cid=0&pn=%d&t=1400495818956'
            m = {'urlTemplate': url_template, 'city': city, 'page': 1}
            url = url_template % (m['city'], m['page'])

            yield Request(url=url, callback=self.parse, meta={'data': m})

    def parse(self, response):

        sel=Selector(response)

        for item in sel.xpath('//div[@class="J_allview-scene"]/article[@class="scene-pic-item"]/div[@class="scene-pic-info"]/p').extract():
            next_url = item['href']
            yield Request(url=next_url)


            pass

        try:
            data = json.loads(response.body)['data']
        except ValueError:
            return

        user_data = response.meta['data']
        page = user_data['page']

        scene_list = data.pop('scene_list')
        if page == 1:
            item = BaiduPoiItem()
            item['data'] = data
            yield item

        for scene in scene_list:
            item = BaiduPoiItem()
            item['data'] = scene
            yield item

        if scene_list:
            page += 1
            user_data['page'] = page

            url_template = user_data['urlTemplate']
            city = user_data['city']
            url = url_template % (city, page)
            yield Request(url=url, callback=self.parse, meta={'data': user_data})