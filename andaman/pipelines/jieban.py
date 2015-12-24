# coding=utf-8
from datetime import datetime

from mongoengine import Document, EmbeddedDocument, EmbeddedDocumentField, StringField, IntField, ListField, connect
import logging

__author__ = 'golmic'


class Comments(EmbeddedDocument):
    # 评论内容
    comment = StringField()

    # 评论作者
    author = StringField()

    # 作者头像Url
    author_avatar = StringField()

    # 评论id
    cid = IntField()


class JiebanDocument(Document):
    # 数据来源
    source = StringField()

    #文章标题
    title = StringField()

    # 出发时间
    startTime = StringField()

    # 预计天数
    days = StringField()

    # 目的地
    destination = ListField()

    # 出发地
    departure = StringField()

    # 预计人数
    groupSize = StringField()

    # 文章描述
    description = StringField()

    # 作者头像Url
    authorAvatar = StringField()

    # 文章id
    tid = IntField()

    # 文章评论
    comments = ListField(EmbeddedDocumentField(Comments))

    #作者
    author = StringField()

    #旅行方式
    type = StringField()


class JiebanPipeline(object):
    @classmethod
    def from_crawler(cls, crawler):
        if not crawler.settings.getbool('PIPELINE_JIEBAN_ENABLED', False):
            from scrapy.exceptions import NotConfigured
            raise NotConfigured
        return cls(crawler.settings)

    def __init__(self, settings):
        self._conn = {}
        self.init_db(settings)

    @staticmethod
    def init_db(settings):
        mongo_uri = settings.get('ANDAMAN_MONGO_URI')
        if mongo_uri:
            return connect(host=mongo_uri)
        else:
            logging.error('Cannot find setting ANDAMAN_MONGO_URI, MongoDB connection is disabled')

    def process_item(self, item, spider):
        source = item['source']
        title = item['title']
        author = item.get('author', '')
        start_time = item['start_time']
        days = item['days']
        destination = item['destination']
        departure = item['departure']
        people = item['people']
        description = item['description']
        author_avatar = item['author_avatar']
        tid = item['tid']
        comments = item['comments']
        ops = {'set__startTime': start_time,
               'set__source': source,
               'set__author': author,
               'set__title': title,
               'set__days': days,
               'set__destination': destination,
               'set__departure': departure,
               'set__groupSize': people,
               'set__description': description,
               'set__comments': comments,
               'set__authorAvatar': author_avatar
            }
        JiebanDocument.objects(tid=tid).update_one(upsert=True, **ops)
        return item
