# coding=utf-8
import random

__author__ = 'zephyre'


class DynamicProxy(object):
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def __init__(self, settings):
        self.crawler_settings = settings
        self.etcd_node = self._get_etcd(settings)
        self.proxies = {}
        self.disabled_proxies = {}

        # 默认情况下，该中间件处于关闭状态
        self.enabled = settings.getbool('DYNAMIC_PROXY_ENABLED', False)
        if self.enabled:
            self.proxies = self.fetch_proxies()

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

    def fetch_proxies(self):
        import requests

        response = requests.get(
            'http://%s:%d/v2/keys/proxies/?recursive=true' % (self.etcd_node['host'], self.etcd_node['port']),
            auth=self.etcd_node['auth'])

        def build_proxy(entry):
            if 'nodes' in entry:
                ret = {v['key'].split('/')[-1]: v['value'] for v in entry['nodes']}
                ret['fail_cnt'] = 0
                if 'timestamp' in ret:
                    ret['timestamp'] = long(ret['timestamp'])
                if 'latency' in ret:
                    ret['latency'] = float(ret['latency'])

                if 'timestamp' in ret and 'latency' in ret:
                    return entry['key'].split('/')[-1], ret
                else:
                    return None
            else:
                return None

        proxies = {tmp[0]: tmp[1] for tmp in map(build_proxy, response.json()['node']['nodes']) if tmp}
        # 去除已经失效的代理服务器
        available_proxies = {tmp[0]: tmp[1] for tmp in proxies.items() if tmp[0] not in self.disabled_proxies}

        return available_proxies

    def _pick_proxy(self, spider):
        """
        随机获得一个代理服务器。如果代理池已空，返回None。
        """
        while True:
            total_cnt = len(self.proxies)
            if total_cnt == 0:
                spider.logger.warning('The dynamic proxy pool is empty.')
                return None
            else:
                idx = random.randint(0, total_cnt-1)
                try:
                    return self.proxies.values()[idx]
                except IndexError:
                    continue

    def process_request(self, request, spider):
        if not self.enabled or 'proxy' in request.meta:
            return

        proxy = self._pick_proxy(spider)
        if proxy:
            request.meta['proxy'] = proxy['proxy']
            request.meta['dynamic_proxy_pool'] = True

    @staticmethod
    def _strip_meta(meta):
        """
        去除meta中可能存在的DynamicProxy的痕迹
        :param meta:
        :return:
        """
        if 'dynamic_proxy_pool' in meta and 'proxy' in meta:
            del meta['proxy']
            del meta['dynamic_proxy_pool']

    def process_response(self, request, response, spider):
        if not self.enabled:
            return response

        self._strip_meta(request.meta)
        return response

    def process_exception(self, request, exception, spider):
        self._strip_meta(request.meta)
        return
