# coding=utf-8
import logging
import random
import threading


__author__ = 'zephyre'


def set_interval(interval):
    """
    定时执行某个函数
    :param interval:
    :return:
    """

    import threading

    def decorator(function):
        def wrapper(*args, **kwargs):
            stopped = threading.Event()

            def loop():  # executed in another thread
                while not stopped.wait(interval):  # until stopped
                    function(*args, **kwargs)

            t = threading.Thread(target=loop)
            t.daemon = True  # stop if the program exits
            t.start()
            return stopped

        return wrapper

    return decorator


class DynamicProxy(object):
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def __init__(self, settings):
        self.logger = logging.getLogger('dynamicproxy')
        self.crawler_settings = settings
        self.etcd_node = self._get_etcd(settings)
        self.proxies = {}
        self.disabled_proxies = set([])
        self._lock = threading.Lock()
        self.fetch_proxies()

        self._schedule = self.fetch_proxies_schedule()

        # 默认情况下，该中间件处于关闭状态
        self.enabled = settings.getbool('DYNAMIC_PROXY_ENABLED', False)

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

    @set_interval(1800)
    def fetch_proxies_schedule(self):
        self.fetch_proxies()

    def fetch_proxies(self):
        import requests

        self.logger.info('Fetching proxies...')

        response = requests.get(
            'http://%s:%d/v2/keys/proxies/?recursive=true' % (self.etcd_node['host'], self.etcd_node['port']),
            auth=self.etcd_node['auth'])

        def is_valid(proxy_info):
            # 取得代理服务器的延迟限制。默认为5秒
            max_latency = self.crawler_settings.get('DYNAMIC_PROXY_MAX_LATENCY', 5)
            return proxy_info['latency'] < max_latency and proxy_info['proxy'] not in self.disabled_proxies

        def build_proxy(entry):
            if 'nodes' in entry:
                ret = {v['key'].split('/')[-1]: v['value'] for v in entry['nodes']}
                ret['fail_cnt'] = 0
                if 'timestamp' in ret:
                    ret['timestamp'] = long(ret['timestamp'])
                if 'latency' in ret:
                    ret['latency'] = float(ret['latency'])

                if is_valid(ret):
                    proxy = ret.pop('proxy')
                    return proxy, ret
                else:
                    return
            else:
                return

        try:
            self._lock.acquire()
            self.proxies.update({tmp[0]: tmp[1] for tmp in map(build_proxy, response.json()['node']['nodes']) if tmp})
        finally:
            self._lock.release()

        self.logger.info('Completed fetching proxies. Total available proxies: %d' % len(self.proxies))

    def deregister(self, proxy, spider):
        """
        注销某个代理
        注意：并非线程安全
        """
        # 将此代理从可用porxies中移出
        self.proxies.pop(proxy)
        # 将此代理放入不可用proxy列表中
        self.disabled_proxies.add(proxy)
        self.logger.warning('Proxy %s disabled. Remaining available proxies: %d' % (proxy, len(self.proxies)))

    def add_fail_cnt(self, proxy, spider):
        """
        登记某个代理的失败次数
        """
        max_fail_cnt = self.crawler_settings.get('DYNAMIC_PROXY_MAX_FAIL', 5)

        try:
            self._lock.acquire()
            self.proxies[proxy]['fail_cnt'] += 1
            if self.proxies[proxy]['fail_cnt'] > max_fail_cnt:
                self.deregister(proxy, spider)
        except KeyError:
            pass
        finally:
            self._lock.release()

    def reset_fail_cnt(self, proxy, spider):
        """
        重置某个代理的失败次数统计
        """
        if self.proxies[proxy]['fail_cnt'] == 0:
            return

        try:
            self._lock.acquire()
            self.proxies[proxy]['fail_cnt'] = 0
        except KeyError:
            pass
        finally:
            self._lock.release()

    def _pick_proxy(self, spider):
        """
        随机获得一个代理服务器。如果代理池已空，返回None。
        """
        while True:
            total_cnt = len(self.proxies)
            if total_cnt == 0:
                spider.logger.warning('The dynamic proxy pool is empty.')
                return
            else:
                idx = random.randint(0, total_cnt - 1)
                try:
                    proxy = self.proxies.keys()[idx]
                    self.logger.debug('Picked proxy: %s' % proxy)
                    return proxy
                except IndexError:
                    continue

    def process_request(self, request, spider):
        # 有ignore_dynamic_proxy标志，且值为True，则不进行加代理操作
        if not self.enabled or 'proxy' in request.meta or 'ignore_dynamic_proxy' in request.meta:
            return

        proxy = self._pick_proxy(spider)
        if proxy:
            request.meta['proxy'] = proxy
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

        meta = request.meta

        # 检测request.meta里的dynamic_proxy_pool值，若没有，则不是此中间件所中，不进行去proxy处理，直接抛出response
        if 'dynamic_proxy_pool' not in meta:
            return response

        # proxy: 'http://host:port'
        proxy = meta['proxy']

        # scrapy中retryMiddleware为500，要处理的http code在retry中处理后，再经过此中间件
        # 此中间件的值要设置的大于500，即下载器下载的response要先经过此中间件，再经过retry中间件
        # 分两种情况处理，一是status大于等于400，判定失败
        # 二是status等于200，但内容不对时也判定失败，这个后面遇到是再加,目前判定为对，抛出response即可

        # 认证标志is_valid
        validator_func = meta['dynamic_proxy_validor'] if 'dynamic_proxy_validator' in meta else lambda _: True
        is_valid = getattr(response, 'status', 500) < 400 and response.body.strip() and validator_func(response)

        # 先只考虑proxy失效的情况，暂定：若是网址失效，由retry处理，网页重试次数由retry处理
        # 对proxy出错的处理：相应的计数加1，去除proxy，抛出requset，
        if is_valid:
            # 某一代理只要有一次成功，则失则次数清0
            self.reset_fail_cnt(proxy, spider)
        else:
            # 失败则相应的计数加1
            self.add_fail_cnt(proxy, spider)
            # 强制让其status为400，重试操作交给retry中间件
            response.status = 400

        self._strip_meta(request.meta)

        return response

    def process_exception(self, request, exception, spider):
        if not self.enabled:
            return

        if 'dynamic_proxy_pool' in request.meta:
            proxy = request.meta['proxy']
            self.add_fail_cnt(proxy, spider)

        self._strip_meta(request.meta)
        return
