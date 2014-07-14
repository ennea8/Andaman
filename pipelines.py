# -*- coding: utf-8 -*-


# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import json
import re
import pymongo


class TravelcrawlerPipeline(object):
    def __init__(self):
        self.client = pymongo.MongoClient('zephyre.me', 27017)
        self.db = self.client.geo
    def process_item(self, item, spider):
        province = item['province']
        city = item['city']
        county = item['county']

        id = item['id']
        data = item['data']

        # 遍历mongo数据库中的locality
        # 先找到city，验证其parent为province
        flag = 0
        for city_obj in self.db.locality.find({'zhName': re.compile(r'^' + city)}):
            if city_obj.get('parent') and (re.compile(r'^' + province)).match(city_obj['parent']['name']):
                # 找到对应的county
                if city_obj.get('siblings'):
                    for t in city_obj['siblings']:
                        if (re.compile(r'^' + county)).match(t['name']):
                            self.db.weather.save({'weatherId': id, 'localityId': t['_id'],
                                             'Data': data, 'province': province, 'city': city, 'county': county})
                            flag = 1
                # 找不到县区级，则尝试找到市级,eg长沙 长沙
                if flag == 0 and city == county:
                    city_id = city_obj.get('_id')
                    self.db.weather.save({'weatherId': id, 'localityId': city_id,
                                     'Data': data, 'province': province, 'city': city, 'county': county})
                    flag = 1
        return item


