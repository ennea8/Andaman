# coding=utf-8
import hashlib
import json
import pymongo
from items import QiniuyunItem

__author__ = 'Jerry'

import scrapy


class PicSpider(scrapy.Spider):
    name = 'pic'
    client = pymongo.MongoClient('zephyre.me', 27017)
    db = client.poi
    start_urls = []
    for t in db.restaurant.find({"imageList": {'$gte': ""}}, {"imageList": 1}):
        start_urls = start_urls + t['imageList']

    def parse(self, response):
        item = QiniuyunItem()
        item['pic'] = response.body
        item['url'] = response._url

        # key = '图片前缀名' + 'url对应的hashcode' + '.后缀名'
        temp_s = str(response._url).split('/')[-1]
        s1 = temp_s.split('.')[0]
        s2 = temp_s.split('.')[-1]
        url_hash = hashlib.md5(response._url).hexdigest()

        item['key'] = 'assets/images/%s.%s.%s' % (s1, url_hash, s2)
        item['hash_value'] = url_hash
        yield item

