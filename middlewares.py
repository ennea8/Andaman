# coding=utf-8
import json
import random
import urllib
import urllib2
import urlparse

from scrapy import log, Request

import conf


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


class GoogleGeocodeMiddleware(object):
    @classmethod
    def from_settings(cls, settings, crawler=None):
        return cls()

    def __init__(self):
        # 读取Google Geocode列表
        self.geocode_keys = {}
        for key in conf.global_conf.get('geocode-keys', {}).values():
            self.geocode_keys[key] = {'key': key, 'fail_cnt': 0, 'over_quota_cnt': 0, 'fail_tot': 0,
                                      'over_quota_tot': 0, 'req_tot': 0, 'enabled': True}

    def random_key(self):
        """
        随机选取一个启用的key
        :return:
        """
        candidates = filter(lambda val: val, map(
            lambda opt: self.geocode_keys[opt]['key'] if self.geocode_keys[opt]['enabled'] else None,
            self.geocode_keys))
        return candidates[random.randint(0, len(candidates) - 1)] if candidates else None

    def process_spider_input(self, response, spider):
        url = response.url
        components = urlparse.urlparse(url)
        if components.netloc == 'maps.googleapis.com' and '/maps/api/geocode/' in components.path:
            try:
                key = response.meta['geocode-key']
                key_entry = self.geocode_keys[key]
                key_entry['req_tot'] += 1

                data = json.loads(response.body)
                status = data['status']
                if status == 'OVER_QUERY_LIMIT':
                    for fd in ('over_quota_cnt', 'fail_cnt', 'over_quota_tot', 'fail_cnt'):
                        key_entry[fd] += 1
                elif status == 'OK' or status == 'ZERO_RESULTS':
                    # 成功一次以后，清空失败统计
                    key_entry['fail_cnt'] = 0
                    key_entry['over_quota_cnt'] = 0
                else:
                    key_entry['fail_cnt'] += 1
                    key_entry['fail_tot'] += 1

                # 连续错误5次以上，禁用这个key
                if key_entry['fail_cnt'] > 20:
                    key_entry['enabled'] = False

            except (KeyError, ValueError):
                pass

    def process_spider_output(self, response, result, spider):
        # 是否需要启用spider
        def _mapper(request):
            if isinstance(request, Request):
                components = urlparse.urlparse(request.url)
                if components.netloc == 'maps.googleapis.com' and '/maps/api/geocode/' in components.path:
                    qs = urlparse.parse_qs(components.query)
                    qs = {k: qs[k][0] for k in qs}
                    if 'key' not in qs:
                        key = self.random_key()
                        if key:
                            qs['key'] = key
                        else:
                            spider.log('NO MORE GEOCODE KEY AVAILABLE!', log.CRITICAL)
                    else:
                        key = qs['key']
                    request.meta['geocode-key'] = key
                    url = urlparse.urlunparse(('https', components.netloc, components.path, components.params,
                                               urllib.urlencode(qs), components.fragment))
                    request = request.replace(url=url)

            return request

        return map(_mapper, result)


class ProxySwitchMiddleware(object):
    @classmethod
    def from_settings(cls, settings, crawler=None):
        latency = settings['PROXY_SWITCH_LATENCY']
        if not latency:
            latency = 0.8
        count = settings['PROXY_SWITCH_COUNT']
        if not count:
            count = 10000
        recently = settings['PROXY_SWITCH_RECENTLY']
        if not recently:
            recently = 12
        auto_refresh = settings['PROXY_SWITCH_REFRESH_INTERVAL']
        if not auto_refresh:
            auto_refresh = 0

        mw_settings = {'latency': latency, 'count': count, 'recently': recently, 'auto_refresh': auto_refresh,
                       'crawler': crawler}

        return cls(mw_settings)

    @classmethod
    def from_crawler(cls, crawler):
        return cls.from_settings(crawler.settings, crawler)

    @staticmethod
    def load_proxy(count, latency, recently):
        response = urllib2.urlopen(
            'http://api2.taozilvxing.cn/core/misc/proxies?verifier=all&latency=%f&recently=%d&pageSize=%d' %
            (latency, recently, count))
        data = json.loads(response.read())
        # 加载代理列表
        proxy_list = {}
        for entry in data['result']:
            host = entry['host']
            scheme = entry['scheme']
            port = entry['port']
            proxy = '%s://%s:%d' % (scheme, host, port)
            # if proxy not in self.disabled_proxies:
            proxy_list[proxy] = {'req': 0, 'fail': 0, 'enabled': True}
        return proxy_list

    def refresh_proxy(self, count, latency, recently):
        spider = getattr(getattr(self, '_crawler'), '_spider')
        if spider:
            spider.log('Proxy spool refreshed: %d available, %d disabled' % (
                len(self.proxy_list), len(self.disabled_proxies)), log.INFO)

        for proxy, proxy_desc in filter(lambda item: item[0] not in self.disabled_proxies,
                                        self.load_proxy(count, latency, recently).items()):
            if proxy not in self.proxy_list:
                self.proxy_list[proxy] = proxy_desc

        self.proxy_list = dict(filter(lambda item: item[0] not in self.disabled_proxies,
                                      self.load_proxy(count, latency, recently).items()))

    def __init__(self, mw_settings):
        crawler = mw_settings['crawler']
        count = mw_settings['count']
        latency = mw_settings['latency']
        recently = mw_settings['recently']
        auto_refresh = mw_settings['auto_refresh']

        self._crawler = crawler
        self.refresh_interval = auto_refresh
        self.disabled_proxies = set([])
        self.proxy_list = dict(filter(lambda item: item[0] not in self.disabled_proxies,
                                      self.load_proxy(count, latency, recently).items()))
        self.max_fail_cnt = 5
        self.max_retry_cnt = 10

        if self.refresh_interval > 0:
            # 每半小时更新一下代理池
            @set_interval(self.refresh_interval)
            def func():
                self.refresh_proxy(count, latency, recently)

            func()

    def deregister(self, proxy, spider):
        """
        注销某个代理
        """
        if proxy in self.proxy_list:
            self.proxy_list.pop(proxy)
            self.disabled_proxies.add(proxy)
            spider.log('Proxy %s disabled' % proxy, log.WARNING)

    def add_fail_cnt(self, proxy, spider):
        """
        登记某个代理的失败次数
        """
        if proxy in self.proxy_list:
            self.proxy_list[proxy]['fail'] += 1
            if self.proxy_list[proxy]['fail'] > self.max_fail_cnt:
                self.deregister(proxy, spider)

    def reset_fail_cnt(self, proxy, spider):
        """
        重置某个代理的失败次数统计
        """
        if proxy in self.proxy_list:
            self.proxy_list[proxy]['fail'] = 0

    def pick_proxy(self):
        """
        从代理池中随机选择一个代理
        :return:
        """
        proxy_list = self.proxy_list.keys()
        if not proxy_list:
            return None
        return proxy_list[random.randint(0, len(proxy_list) - 1)]

    def process_request(self, request, spider):
        proxy = self.pick_proxy()
        if proxy:
            spider.log('Set proxy %s for request: %s' % (proxy, request.url), log.DEBUG)
            request.meta['proxy'] = proxy
            request.meta['proxy_middleware'] = self
            if 'proxy_switch_ctx' not in request.meta:
                request.meta['proxy_switch_ctx'] = {}
            if 'request_cnt' not in request.meta['proxy_switch_ctx']:
                request.meta['proxy_switch_ctx']['request_cnt'] = 0
            self.proxy_list[proxy]['req'] += 1

    def process_response(self, request, response, spider):
        if 'proxy' not in request.meta:
            return response
        proxy = request.meta['proxy']

        ctx = request.meta['proxy_switch_ctx']

        if 'validator' in ctx and ctx['validator']:
            is_valid = ctx['validator'](response)
        else:
            is_valid = response.status == 200 or response.status not in (301, 302)

        # 如果返回代码不为200，则表示出错
        if not is_valid:
            request.meta['proxy_switch_ctx']['request_cnt'] += 1
            self.add_fail_cnt(proxy, spider)
            if request.meta['proxy_switch_ctx']['request_cnt'] < self.max_retry_cnt:
                return request
            else:
                return response
        else:
            self.reset_fail_cnt(proxy, spider)
            return response

    def process_exception(self, request, exception, spider):
        # 代理失败统计
        if 'proxy' not in request.meta:
            return

        request.meta['proxy_switch_ctx']['request_cnt'] += 1
        proxy = request.meta['proxy']
        self.add_fail_cnt(proxy, spider)
        if request.meta['proxy_switch_ctx']['request_cnt'] < self.max_retry_cnt:
            return request