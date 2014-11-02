# coding=utf-8
import json
import random
import urllib2

from scrapy import log


__author__ = 'zephyre'


class ProxySwitchMiddleware(object):
    @classmethod
    def from_settings(cls, settings, crawler=None):
        verifier = settings['PROXY_SWITCH_VERIFIER']
        if not verifier:
            verifier = 'baidu'
        latency = settings['PROXY_SWITCH_LATENCY']
        if not latency:
            latency = 5
        count = settings['PROXY_SWITCH_COUNT']
        if not count:
            count = 100
        recently = settings['PROXY_SWITCH_RECENTLY']
        if not recently:
            recently = 12

        return cls(verifier, latency, recently, count)

    @classmethod
    def from_crawler(cls, crawler):
        return cls.from_settings(crawler.settings, crawler)

    def __init__(self, verifier, latency, recently, count):
        response = urllib2.urlopen(
            'http://api.lvxingpai.cn/core/misc/proxies?verifier=%s&latency=%d&recently=%d&pageSize=%d' %
            (verifier, latency, recently, count))
        data = json.loads(response.read())

        # 加载代理列表
        proxy_list = {}

        for entry in data['result']:
            host = entry['host']
            scheme = entry['scheme']
            port = entry['port']
            proxy = '%s://%s:%d' % (scheme, host, port)
            proxy_list[proxy] = {'req': 0, 'fail': 0, 'enabled': True}

        self.proxy_list = proxy_list

    def pick_proxy(self):
        proxy_list = filter(lambda val: self.proxy_list[val]['enabled'], self.proxy_list.keys())
        if not proxy_list:
            return None
        return proxy_list[random.randint(0, len(proxy_list) - 1)]

    def process_request(self, request, spider):
        proxy = self.pick_proxy()
        if proxy:
            request.meta['proxy'] = proxy
            self.proxy_list[proxy]['req'] += 1

        return

    def process_response(self, request, response, spider):
        # # 如果返回代码不为200，则表示出错
        # if response.status != 200 and response.status not in (301, 302, 404, 500, 503):
        # # request.meta['proxySwitchStat']['reqCount'] += 1
        # # 代理失败统计
        # if 'proxy' in request.meta:
        # proxy = request.meta['proxy']
        # if proxy in self.proxy_list:
        # d = self.proxy_list[proxy]
        # d['fail'] += 1
        # # 代理是否存活？
        # if float(d['fail']) / float(d['req']) > 0.7:
        # d['enabled'] = False
        #
        # return request

        if 'proxy' in request.meta:
            proxy = request.meta['proxy']
            if proxy in self.proxy_list:
                self.proxy_list[proxy]['fail'] = 0

        return response

    def process_exception(self, request, exception, spider):
        # 代理失败统计
        if 'proxy' in request.meta:
            proxy = request.meta['proxy']
            if proxy in self.proxy_list:
                d = self.proxy_list[proxy]
                d['fail'] += 1
                if d['fail'] >= 5:
                    spider.log('PROXY %s IS DISABLED.' % proxy, log.WARNING)
                    d['enabled'] = False

        return request