# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import pymongo
from scrapy.exceptions import DropItem
from andaman.items.baidu import BaiduNoteItem

class AndamanPipeline(object):
    spiders = []
    # 按照文档，进行基本的实现

    def __init__(self, mongo_host, mongo_port, mongo_user, mongo_passwd, mongo_dbname):
        self.mongo_host = mongo_host
        self.mongo_port = mongo_port
        self.mongo_user = mongo_user
        self.mongo_passwd = mongo_passwd
        self.mongo_dbname = mongo_dbname

    # used for local test
    # def __init__(self, mongo_uri, mongo_port):
    #     self.mongo_uri = mongo_uri
    #     self.mongo_port = mongo_port

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_host=crawler.settings.get('MONGO_HOST'),
            mongo_port=crawler.settings.get('MONGO_PORT'),
            mongo_user=crawler.settings.get('MONGO_USER'),
            mongo_passwd=crawler.settings.get('MONGO_PASSWD'),
            mongo_dbname=crawler.settings.get('MONGO_DBNAME')

            # used for local test
            # mongo_uri=crawler.settings.get('MONGO_URI'), #'localhost'
            # mongo_port=crawler.settings.get('MONGO_PORT') #27017
        )

    def open_spider(self, spider):
        uri_one = 'mongodb://'+self.mongo_user[0]+':'+self.mongo_passwd[0]+'@'+self.mongo_host[0]
        uri_two = ':'+self.mongo_port[0]+'/'+self.mongo_dbname[0]
        uri = uri_one + uri_two
        self.conn = pymongo.MongoClient(uri)

        # used for local test
        # self.conn = pymongo.MongoClient(self.mongo_uri, self.mongo_port)

    def close_spider(self, spider):
        self.conn.close()

    def process_item(self, item, spider):
        return item


class BaiduNotePipeline(AndamanPipeline):

    def process_item(self, item, spider):
        if type(item).__name__ == 'BaiduNoteItem':
            self.spiders = spider.name

            # 连接db,建Spider的db
            db = self.conn[self.mongo_dbname[0]]
            # used for local test
            # db = self.conn[spider.name]
            one_note = item['one_note']
            query = {'note_id': one_note['note_id']}
            if isinstance(item, BaiduNoteItem):
                coll = db['BaiduNoteItem']
                coll.update(query, {'$set': one_note}, upsert=True)
                # 更新此条数据后，紧接着开始清洗

        return item


