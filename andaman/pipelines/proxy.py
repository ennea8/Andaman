# coding=utf-8
from datetime import datetime

from mongoengine import Document, EmbeddedDocument, EmbeddedDocumentField, StringField, IntField, FloatField, \
    DateTimeField, MapField, connect
import logging

__author__ = 'zephyre'


class ValidationResult(EmbeddedDocument):
    # 延迟大小，单位为秒
    latency = FloatField(min_value=0, required=True)

    # 更新时间戳
    val_time = DateTimeField(db_field='valTime', required=True)


class ProxyDocument(Document):
    # 代理服务器的scheme, 取值为http或https
    scheme = StringField(required=True, default='http', choices=['http', 'https'])

    # 代理服务器的地址
    host = StringField(required=True)

    # 代理服务器的端口
    port = IntField(min_value=1, max_value=65535, required=True)

    # 代理服务器的用户名（可选）
    user = StringField(null=True)

    # 代理服务器的密码（可选）
    password = StringField(null=True)

    # 代理服务器的描述
    desc = StringField(null=True)

    # 最后一次更新验证结果的时间
    last_mod = DateTimeField(db_field='lastMod', required=True)

    # 代理服务器的验证结果，key为验证对象的名称，比如baidu，google等
    validation = MapField(field=EmbeddedDocumentField(ValidationResult))

    meta = {'collection': 'Proxy',
            'indexes': [{'fields': ['scheme', 'host', 'port'], 'unique': True},
                        {'fields': ['last_mod'], 'expireAfterSeconds': 3600 * 24 * 7}]}


class ProxyPipeline(object):
    @classmethod
    def from_crawler(cls, crawler):
        if not crawler.settings.getbool('PIPELINE_PROXY_ENABLED', False):
            from scrapy.exceptions import NotConfigured
            raise NotConfigured
        return cls()

    def __init__(self):
        self._conn = {}

    @staticmethod
    def init_db(settings):
        """
        初始化MongoDB数据库连接
        :param settings:
        :return:
        """
        mongo_uri = settings.get('ANDAMAN_MONGO_URI')
        if mongo_uri:
            return connect(host=mongo_uri)
        else:
            logging.error('Cannot find setting ANDAMAN_MONGO_URI, MongoDB connection is disabled')

    def process_item(self, item, spider):
        # 惰性初始化数据库
        settings = spider.crawler.settings
        spider_name = spider.name
        if spider_name not in self._conn:
            conn = self.init_db(settings)
            if conn:
                self._conn[spider_name] = conn
        else:
            conn = self._conn[spider_name]
        if not conn:
            return item

        scheme = item['scheme']
        host = item['host']
        port = item['port']
        proxy = '%s://%s:%d' % (scheme, host, port)
        desc = item['desc']
        validate_by = item['validate_by']
        validate_time = item['validate_time']
        latency = item['latency']

        action = item['action']
        if action == 'discard':
            spider.logger.debug('Removing proxy: %s' % proxy)
            ops = {'unset__validation__%s' % validate_by: 1, 'set__last_mod': datetime.utcnow()}
            ProxyDocument.objects(scheme=scheme, host=host, port=port).update_one(**ops)
        elif action == 'update_latency':
            spider.logger.debug('Updating existing proxy: %s' % proxy)
            ops = {'set__validation__%s__latency' % validate_by: latency, 'set__last_mod': validate_time}
            ProxyDocument.objects(scheme=scheme, host=host, port=port).update_one(**ops)
        elif action == 'default':
            spider.logger.debug('Adding proxy: %s' % proxy)
            ops = {'set__desc': desc,
                   'set__validation__%s__latency' % validate_by: latency,
                   'set__validation__%s__val_time' % validate_by: validate_time,
                   'set__last_mod': validate_time}
            ProxyDocument.objects(scheme=scheme, host=host, port=port).update_one(upsert=True, **ops)

        return item

