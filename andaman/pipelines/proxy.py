# coding=utf-8
import hashlib

import requests
from scrapy.exceptions import DropItem


__author__ = 'zephyre'


class ProxyPipeline(object):
    spiders = ['youdaili']

    def __init__(self):
        import logging

        logging.getLogger("requests").setLevel(logging.WARNING)

    @staticmethod
    def _get_etcd(settings):
        return {'host': settings.get('ETCD_HOST', 'etcd'), 'port': settings.getint('ETCD_PORT', 2379)}

    @staticmethod
    def _get_auth(settings):
        from requests.auth import HTTPBasicAuth

        user = settings.get('ETCD_USER', '')
        password = settings.get('ETCD_PASSWORD', '')

        if user and password:
            return HTTPBasicAuth(user, password)
        else:
            return None

    def process_item(self, item, spider):
        if getattr(spider, 'name', '') not in self.spiders:
            return item

        settings = spider.crawler.settings
        etcd_node = self._get_etcd(settings)

        host = item['host']
        port = item['port']
        scheme = item['scheme']
        proxy = '%s://%s:%d' % (scheme, host, port)

        m = hashlib.md5(proxy)
        digest = m.hexdigest()

        key = '/proxies/%s' % digest
        base_url = 'http://%s:%d/v2/keys%s' % (etcd_node['host'], etcd_node['port'], key)

        # 默认每个代理服务器项目存活3天
        ttl = settings.getint('ETCD_TTL', 3600 * 24 * 3)

        auth = self._get_auth(settings)

        try:
            self._process_response(requests.put(base_url, auth=auth, data={'ttl': ttl, 'dir': True}), spider)
            self._process_response(requests.put(base_url + '/proxy', auth=auth, data={'value': proxy}), spider)
            self._process_response(requests.put(base_url + '/desc', auth=auth, data={'value': item['desc']}), spider)
            self._process_response(requests.put(base_url + '/latency', auth=auth, data={'value': item['latency']}),
                                   spider)
            self._process_response(
                requests.put(base_url + '/timestamp', auth=auth, data={'value': item['verifiedTime']}), spider)
            return item
        except IOError:
            raise DropItem

    @staticmethod
    def _process_response(response, spider):
        status_code = response.status_code

        if status_code != 200:
            if status_code == 401:
                spider.logger.warning('Insufficient credentials')
                raise IOError
            else:
                spider.logger.warning('Something went wrong with the etcd service')
                raise IOError

