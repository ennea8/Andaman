# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import json
import re
import pymongo
import sys

reload(sys)
sys.setdefaultencoding("utf-8")


class TravelcrawlerPipeline(object):
    def __init__(self):
        client = pymongo.MongoClient('zephyre.me', 27017)
        self.db = client.geo

    def process_item(self, item, spider):
        province = item['province']
        city = item['city']
        county = item['county']

        id = item['id']
        data = item['data']

        # 先找到city，验证其parent为province
        city_obj = self.db.locality.find_one({'zhName': re.compile(r'^' + city),
                                              'parent.name': re.compile(r'^' + province)})
        if city_obj:
            # 找到对应的county
            if 'siblings' in city_obj:
                for t in city_obj['siblings']:
                    if (re.compile(r'^' + county)).match(t['name']):
                        county_id = t['_id']
                        self.db.weather.save({'weatherId': id, 'localityId': county_id,
                                              'Data': data, 'province': province, 'city': city, 'county': county})
                        return item

            # 找不到则为对应的城市Id eg. 长沙 长沙
            if '_id' in city_obj and city == county:
                city_id = city_obj['_id']
                self.db.weather.save({'weatherId': id, 'localityId': city_id,
                                      'Data': data, 'province': province, 'city': city, 'county': county})
                return item
        return item



class MofengwoPipeline(object):
    def __init__(self):
        client = pymongo.MongoClient('localhost', 27017)
        self.db = client.mydb

    def process_item(self, item, spider):
        author_id = item['author_id']
        author_name= item['author_name']
        url = item['url']
        date = item['date']
        tag = item['tag']
        keyword= item['keyword']
        desc= item['desc']
        img= item['img']
        self.db.mafengwo.save({'author_id':author_id,'author_name':author_name,'tag':tag,'keyword':keyword,'desc':desc,'img':img,'url':url,'date':date})
        return item


