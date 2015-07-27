# coding=utf-8
import random
from scrapy import log

__author__ = 'zephyre'


class DynamicProxy(object):
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def __init__(self, settings):
        self.crawler_settings = settings
        self.etcd_node = self._get_etcd(settings)
        self.proxies = {}
        self.proxies_dict = {}
        self.disabled_proxies = {}
        self.max_fail_cnt = 5

        # 默认情况下，该中间件处于关闭状态
        self.enabled = settings.getbool('DYNAMIC_PROXY_ENABLED', False)
        if self.enabled:
            self.proxies = self.fetch_proxies()
            # 生成proxy:key的字典
            self.proxies_dict = {tmp[1]['proxy']: tmp[0] for tmp in self.proxies.items()}

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

        # r1 = requests.get('http://www.baidu.com')

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

    def deregister(self, proxies_key, spider):
        """
        注销某个代理
        """
        if proxies_key in self.proxies:
            # 将此代理从可用porxies中移出
            self.proxies.pop(proxies_key)
            # 将此代理放入不可用proxy列表中
            self.disabled_proxies.setdefault(proxies_key)
            spider.log('Proxy %s disabled' % proxies_key, log.WARNING)

    def add_fail_cnt(self, proxy, spider):
        """
        登记某个代理的失败次数
        """
        proxies_key = self.proxies_dict[proxy]
        if proxies_key:
            self.proxies[proxies_key]['fail_cnt'] += 1
            # 失则次数超过最大值则注销此代理
            if self.proxies[proxies_key]['fail_cnt'] > self.max_fail_cnt:
                self.deregister(proxies_key, spider)

    def reset_fail_cnt(self, proxy, spider):
        """
        重置某个代理的失败次数统计
        """
        if proxy in self.proxies_dict:
            proxies_key = self.proxies_dict[proxy]
            self.proxies[proxies_key]['fail_cnt'] = 0

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
                    # self.proxies: {'key': ret}, 问题就是proxies的key是什么，别的都好办
                    # values: [ret{'proxy':  , 'fail_cnt': }, ...]
                    return self.proxies.values()[idx]
                except IndexError:
                    continue

    def process_request(self, request, spider):
        if not self.enabled or 'proxy' in request.meta:
            return

        proxy = self._pick_proxy(spider)
        # proxy= ret{'proxy': , 'fail_cnt':  }
        if proxy:
            request.meta['proxy'] = proxy['proxy']
            request.meta['add_proxy'] = True

    def process_response(self, request, response, spider):
        if not self.enabled:
            return response
        # 检测request.meta里的add_proxy值，若没有，则不是此中间件所中，不进行去proxy处理，直接抛出response
        try:
            # 若为真，则说明代理为此中件间所加，连同加上的代理一起去掉
            # 此时，应去掉request.meta中的内容，response.meta此时是空
            if request.meta['add_proxy']:
                # 先存下proxy，做后续判断是否失败处理
                # proxy: 'http://host:port'
                proxy = request.meta['proxy']
                request.meta.pop('proxy')
                request.meta.pop('add_proxy')
        except KeyError:
            return response
        except AttributeError:
            return response

        # scrapy中retryMiddleware为500，要处理的http code在retry中处理后，再经过此中间件
        # 此中间件的值要设置的大于500，即下载器下载的response要先经过此中间件，再经过retry中间件
        # 分两种情况处理，一是status大于等于400，判定失败
        #                二是status等于200，但内容不对时也判定失败，这个后面遇到是再加,目前判定为对，抛出response即可

        # 认证标志is_valid
        is_valid = False
        if response.status < 400:
            status_valid = True
            # 如果response的http statuscode正确
            is_valid = status_valid

        # 用来判断是否含有valid函数
        try:
            # response中对proxy是否有效的判断
            if 'valid' in request.meta:
                valid_bool = request.meta['valid'](response)
                is_valid = status_valid and valid_bool

        except KeyError:
            pass

        # 先只考虑proxy失效的情况，暂定：若是网址失效，由retry处理，网页重试次数由retry处理
        # 对proxy出错的处理：相应的计数加1，去除proxy，抛出requset，
        # 含status == 200的情况
        if is_valid:
            # 某一代理只要有一次成功，则失则次数清0
            self.reset_fail_cnt(proxy, spider)
        else:
            # 失败则相应的计数加1
            self.add_fail_cnt(proxy, spider)

        return response

    def process_exception(self, request, exception, spider):
        if not self.enabled:
            return

        try:
            if request.meta['add_proxy']:
                proxy = request.meta['proxy']
                self.add_fail_cnt(proxy, spider)
                # 处理exception也要去掉proxy，为了方便scrapy retry中间件的处理
                request.meta.pop('proxy')
                request.meta.pop('add_proxy')

        except KeyError:
            pass

        return
