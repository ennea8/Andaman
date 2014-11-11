# coding=utf-8
import json
import random
import urllib
import urllib2
import urlparse

from scrapy import log, Request

import utils


__author__ = 'zephyre'


class GoogleGeocodeMiddleware(object):
    @classmethod
    def from_settings(cls, settings, crawler=None):
        return cls()

    def __init__(self):
        # 读取Google Geocode列表
        section = 'geocode-keys'
        self.geocode_keys = {}
        for option in utils.cfg_options(section):
            key = utils.cfg_entries(section, option)
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

                self.geocode_keys['req_tot'] += 1

                data = json.loads(response.body)
                status = data['status']
                if status == 'OVER_QUERY_LIMIT':
                    for fd in ('over_quota_cnt', 'fail_cnt', 'over_quota_tot', 'fail_cnt'):
                        key_entry[fd] += 1
                elif status == 'OK':
                    # 成功一次以后，清空失败统计
                    key_entry['fail_cnt'] = 0
                    key_entry['over_quota_cnt'] = 0
                else:
                    key_entry['fail_cnt'] += 1
                    key_entry['fail_tot'] += 1

                # 连续错误5次以上，禁用这个key
                if key_entry['fail_cnt'] > 5:
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
                    url = urlparse.urlunparse((components.scheme, components.netloc, components.path, components.params,
                                               urllib.urlencode(qs), components.fragment))
                    request.replace(url=url)

            return request

        return map(_mapper, result)


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