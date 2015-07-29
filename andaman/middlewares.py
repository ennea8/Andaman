# coding=utf-8
# 此中间件功能：给每一个request加上代理，防止抓取内容时某些网站封IP
# 主要分为两部分：
# 1.process_request:处理每个request时加上代理
# 2.process_response:通过判断每个request返回的response来判断此代理能否正常工作
#   若不正常工作，则抛出此request，重新加上新的代理，再试一次
#   设定重试的次数，超过则失败,返回此response即可
# 在海子的代码上作更改，能跑起来就好。主要是添加process_response的处理

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

class ProxySwitchMiddleware(object):
    @classmethod
    def from_settings(cls, settings, crawler=None):
        latency = settings['PROXY_SWITCH_LATENCY']
        if not latency:
            latency = 1
        count = settings['PROXY_SWITCH_COUNT']
        if not count:
            count = 10000
        recently = settings['PROXY_SWITCH_RECENTLY']
        if not recently:
            recently = 24
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
            'http://api.taozilvxing.cn/core/misc/proxies?verifier=all&latency=%f&recently=%d&pageSize=%d' %
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
            # 将此代理从可用porxy_list中移出
            self.proxy_list.pop(proxy)
            # 将此代理放入不可用proxy列表中
            self.disabled_proxies.add(proxy)
            spider.log('Proxy %s disabled' % proxy, log.WARNING)

    def add_fail_cnt(self, proxy, spider):
        """
        登记某个代理的失败次数
        """
        if proxy in self.proxy_list:
            self.proxy_list[proxy]['fail'] += 1
            # 失则次数超过最大值则注销此代理
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
        # 如何临时性禁用proxy middleware: 将meta['proxy_middleware']['enabled']设置为False
        mw_key = 'proxy_switch_ctx'
        try:
            if mw_key in request.meta and request.meta[mw_key]['enabled'] is False:
                return
        except KeyError:
            pass

        proxy = self.pick_proxy()
        if proxy:
            spider.log('Set proxy %s for request: %s' % (proxy, request.url), log.DEBUG)
            # 给request加上一个proxy
            request.meta['proxy'] = proxy
            request.meta['proxy_middleware'] = self
            # proxy_switch_ctx用来记录request的次数，若达到最大值，则说明失效，返回此request产生的response
            if 'proxy_switch_ctx' not in request.meta:
                request.meta['proxy_switch_ctx'] = {}
            if 'request_cnt' not in request.meta['proxy_switch_ctx']:
                request.meta['proxy_switch_ctx']['request_cnt'] = 0
            self.proxy_list[proxy]['req'] += 1

    def process_response(self, request, response, spider):
        if 'proxy' not in request.meta:
            return response
        mw_key = 'proxy_switch_ctx'
        try:
            # 为flase则禁用此middleware
            if mw_key in request.meta and request.meta[mw_key]['enabled'] is False:
                return response
        except KeyError:
            pass

        proxy = request.meta['proxy']

        # ctx = request.meta['proxy_switch_ctx']

        # validator是什么没看出来
        # if 'validator' in ctx and ctx['validator']:
        #     is_valid = ctx['validator'](response)
        # else:
        #     is_valid = response.status == 200 or response.status not in (301, 302)

        # response中对proxy是否有效的主要判断
        # scrapy中retryMiddleware为500，要处理的http code在retry中处理后，再经过此中间件
        # 相关的参数要改一改，如RETRY_TIMES设置成和maxretry相同之类的
        # 此中间件的值要设置的小于500，即下载器下载的response要先经过retry中间件
        is_valid = response.status == 200 or response.status not in (301.302)

        # 若返回代码为200，也要检测response的body内容是不是某些网站的封ip的消息显示
        # if is_valid:
            # 检测response的内容，考虑用正则匹配
            # 此处考虑建立一个过滤库，专门用来收录各网站的各种封ip显示方式
            # if failure in filter(site_false_func(), response.body):
            # if site_false_func(response.body)
            #     is_valid = False
            # 若检测得到则说明出错

        # 若is_valid为false，说明出错
        if not is_valid:
            request.meta['proxy_switch_ctx']['request_cnt'] += 1
            self.add_fail_cnt(proxy, spider)
            if request.meta['proxy_switch_ctx']['request_cnt'] < self.max_retry_cnt:
                return request
            else:
                return response
        else:
            # 某一代理只要有一次成功，则失则次数清0
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