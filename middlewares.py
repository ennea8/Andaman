# coding=utf-8
import json
import random
import urllib2
import time

from scrapy import log


__author__ = 'zephyre'


class TestMiddleware(object):
    def process_request(self, request, spider):
        request.meta['test'] = 1
        return None

    def process_response(self, request, response, spider):
        # response.meta['test2'] = 1
        return response


class TestMiddleware2(object):
    def process_request(self, request, spider):
        request.meta['testa'] = 1
        return None

    def process_response(self, request, response, spider):
        # response.meta['testa2'] = 1
        return response


class ProxySwitchMiddleware(object):
    def __init__(self):
        data = None
        for retry_idx in xrange(3):
            try:
                response = urllib2.urlopen('http://cms.lvxingpai.cn/proxy.json')
                data = json.loads(response.read())
                break
            except IOError:
                if retry_idx < 3:
                    time.sleep(2)
                else:
                    break

        if not data:
            data = []

        # 加载代理列表
        proxy_list = {}
<<<<<<< HEAD

        with open('/home/wdx/travelpicrawler/data/proxy_list.txt', 'r') as f:
            for line in f:
                proxy_list['http://' + line.strip()] = {'req': 0, 'fail': 0, 'enabled': True}
=======
        for line in data:
            proxy_list['http://' + line.strip()] = {'req': 0, 'fail': 0, 'enabled': True}
>>>>>>> 847ff49111c50287f01cbd0613e4dc988d1df681
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
        #             d['fail'] += 1
        #             # 代理是否存活？
        #             if float(d['fail']) / float(d['req']) > 0.7:
        #                 d['enabled'] = False
        #
        #     return request

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