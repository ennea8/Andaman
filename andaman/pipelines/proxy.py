# coding=utf-8
import hashlib

import requests


__author__ = 'zephyre'


class ProxyPipeline(object):
    spiders = ['youdaili']

    def __init__(self):
        import logging

        logging.getLogger("requests").setLevel(logging.WARNING)

    @staticmethod
    def _get_etcd(settings):
        return {'host': settings.get('ETCD_HOST', 'etcd'), 'port': settings.getint('ETCD_PORT', 2379)}

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

        requests.put(base_url, data={'ttl': ttl, 'dir': True})
        requests.put(base_url + '/proxy', data={'value': proxy})
        requests.put(base_url + '/desc', data={'value': item['desc']})
        requests.put(base_url + '/latency', data={'value': item['latency']})
        requests.put(base_url + '/timestamp', data={'value': item['verifiedTime']})

        return item
