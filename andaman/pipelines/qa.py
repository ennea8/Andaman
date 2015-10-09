# coding=utf-8
import requests

__author__ = 'zephyre'


class QAPipeline(object):
    @classmethod
    def from_crawler(cls, crawler):
        if not crawler.settings.getbool('PIPELINE_QA_ENABLED', False):
            from scrapy.exceptions import NotConfigured
            raise NotConfigured
        return cls(crawler.settings)

    @staticmethod
    def _get_mongo_uri(etcd_node):
        """
        通过etcd服务，获得MongoDB的信息
        :param etcd_node:
        :return:
        """
        auth = etcd_node['auth']
        host = etcd_node['host']
        port = etcd_node['port']

        base_url = 'http://%s:%d/v2/keys' % (host, port)

        db = requests.get(base_url + '/project-conf/andaman/mongo/db', auth=auth).json()['node']['value']
        user = requests.get(base_url + '/project-conf/andaman/mongo/user', auth=auth).json()['node']['value']
        password = requests.get(base_url + '/project-conf/andaman/mongo/password', auth=auth).json()['node']['value']

        # 获得MongoDB集群信息
        mongo_service = requests.get(base_url + '/project-conf/andaman/mongo/service', auth=auth).json()['node'][
            'value']
        mongo_replicaset = requests.get(base_url + '/project-conf/andaman/mongo/replicaset', auth=auth).json()['node'][
            'value']
        nodes = requests.get(base_url + '/backends/%s/?recursive=true' % mongo_service, auth=auth).json()['node'][
            'nodes']
        mongo_addrs = ','.join([tmp['value'] for tmp in nodes])
        mongo_uri = 'mongodb://%s:%s@%s/%s?replicaSet=%s&readPreference=primaryPreferred' \
                    % (user, password, mongo_addrs, db, mongo_replicaset)

        return mongo_uri

    @staticmethod
    def _get_mongo_db(mongo_uri):
        from pymongo import MongoClient

        client = MongoClient(mongo_uri)
        return client.andaman

    def __init__(self, settings):
        self.etcd_node = self._get_etcd(settings)
        self.mongo = None

    @staticmethod
    def _get_etcd(settings):
        from requests.auth import HTTPBasicAuth

        user = settings.get('ETCD_USER', '')
        password = settings.get('ETCD_PASSWORD', '')

        if user and password:
            auth = HTTPBasicAuth(user, password)
        else:
            auth = None

        return {'host': settings.get('ETCD_HOST', 'etcd'), 'port': settings.getint('ETCD_PORT', 2379), 'auth': auth}

    def process_item(self, item, spider):
        return self.process_qa(item, spider)

    def process_qa(self, item, spider):
        data = {}
        for field in item.fields:
            if field in ['files', 'file_urls']:
                continue
            try:
                data[field] = item[field]
            except KeyError:
                pass

        # 处理图像
        image_map = {v['url']: v['path'] for v in item.get('files', [])}

        avatar = item.get('author_avatar', None)
        if avatar and avatar in image_map:
            data['author_avatar'] = image_map[avatar]
        else:
            try:
                del data['author_avatar']
            except KeyError:
                pass

        if not self.mongo:
            self.mongo = self._get_mongo_db(self._get_mongo_uri(self.etcd_node))

        col = self.mongo.QA
        aid = item.get('aid', None)
        col.update({'qid': item['qid'], 'aid': aid}, {'$set': data}, upsert=True)

        return item
