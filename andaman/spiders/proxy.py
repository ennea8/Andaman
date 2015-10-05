# coding=utf-8
import json
from urlparse import urljoin
import re
from datetime import datetime

import scrapy
from scrapy.http import Request

from andaman.items.proxy import ProxyItem


__author__ = 'zephyre'


class YoudailiSpider(scrapy.Spider):
    """
    通过youdaili.net网站，抓取代理服务器列表

    相关设置：
    * YOUDAILI_TARGET: 目前仅支持web，默认为web
    * YOUDAILI_PAGES_CNT: 抓取网站前多少页的代理服务器数据，默认为1
    * YOUDAILI_REQUEST_CNT: 验证代理服务器的时候，访问多次，将每一次访问的延迟综合起来，取其平均值，默认为5次
    * YOUDAILI_MATCH: 验证代理服务器所使用的字符串
    """
    name = 'youdaili'

    def __init__(self, *args, **kwargs):
        super(YoudailiSpider, self).__init__(*args, **kwargs)

        # 已经处理过的代理服务器
        self.processed_proxies = set([])

    def start_requests(self):
        settings = self.crawler.settings

        # 代理服务器列表的来源：
        # web: 表示从youdaili.net网站获取
        # etcd: 表示从etcd服务器中获取
        target = settings.get('YOUDAILI_TARGET', 'web')

        if target == 'web':
            pages_cnt = settings.getint('YOUDAILI_PAGES_CNT', 1)
            for url in ['http://www.youdaili.net/Daili/guonei/list_%d.html' % page for page in
                        xrange(1, pages_cnt + 1)]:
                yield Request(url=url, callback=self.parse)

    def update_proxies(self, response):
        from urlparse import urlparse

        def build_proxy(entry):
            if 'nodes' in entry:
                ret = {v['key'].split('/')[-1]: v['value'] for v in entry['nodes']}
                if 'timestamp' in ret:
                    ret['timestamp'] = long(ret['timestamp'])
                if 'latency' in ret:
                    ret['latency'] = float(ret['latency'])

                if 'timestamp' in ret and 'latency' in ret:
                    return ret
                else:
                    return None
            else:
                return None

        proxies = [tmp for tmp in map(build_proxy, json.loads(response.body)['node']['nodes']) if tmp]

        for proxy in proxies:
            desc = proxy['desc']
            proxy_url = proxy['proxy']

            # 跳过已经处理过的proxy
            if proxy_url in self.processed_proxies:
                continue

            parse_result = urlparse(proxy_url)
            netloc = parse_result.netloc
            tmp = netloc.split(':')
            if len(tmp) != 2:
                continue
            host, port = tmp[0], int(tmp[1])

            yield Request(url='http://www.baidu.com', callback=self.verify, dont_filter=True,
                          errback=self.verify_errback,
                          meta={'host': host, 'port': port, 'desc': desc, 'latency_results': [],
                                'update_existing': True,
                                'proxy': 'http://%s:%d' % (host, port),
                                'dont_redirect': True, 'dont_retry': True, 'download_timeout': 10})

    def parse(self, response):
        """
        处理：http://www.youdaili.net/Daili/http/list_1.html
        """
        for href in response.selector.xpath(
                '//div[@class="newslist_body"]/ul[@class="newslist_line"]/li/a[@href]/@href'):
            url = urljoin(response.url, href.extract())
            yield Request(url=url, callback=self.parse_proxy_list, meta={'page': 1})

    def parse_proxy_list(self, response):
        """
        处理：http://www.youdaili.net/Daili/http/3449_1.html
        :param response:
        :return:
        """
        # 如果是第一页，需要处理分页
        if response.meta['page'] == 1:
            for node in response.selector.xpath('//div[@class="dede_pages"]/ul[@class="pagelist"]/li/a[@href]'):
                try:
                    page_num = int(node.xpath('./text()').extract()[0])
                    if page_num != 1:
                        html_name = node.xpath('./@href').extract()[0]
                        url = urljoin(response.url, html_name)
                        yield Request(url=url, callback=self.parse_proxy_list, meta={'page': page_num})
                except ValueError:
                    pass

        # 获得列表
        for raw_entry in [tmp.strip() for tmp in
                          response.selector.xpath('//div[@class="cont_font"]/p/text()').extract()]:
            pattern = r'(\d+\.\d+\.\d+\.\d+):(\d+)@HTTP#(.+)'
            m = re.match(pattern, raw_entry)
            if m:
                host = m.group(1)
                port = int(m.group(2))
                desc = m.group(3)

                key = 'http://%s:%d' % (host, port)
                if key in self.processed_proxies:
                    continue

                yield Request(url='http://www.baidu.com', callback=self.verify, dont_filter=True,
                              errback=self.verify_errback,
                              meta={'host': host, 'port': port, 'desc': desc, 'latency_results': [],
                                    'proxy': 'http://%s:%d' % (host, port),
                                    'update_existing': False,
                                    'dont_redirect': True, 'dont_retry': True, 'download_timeout': 10})

    def verify_errback(self, failure):
        # 如果当前处于update_existing模式，如果连续出错3次，出错的代理需要去掉
        request = failure.request
        meta = request.meta

        if meta['update_existing']:
            settings = self.crawler.settings
            req_cnt = settings.getint('YOUDAILI_REQUEST_CNT', 5)
            fail_cnt = (meta['fail_cnt'] if 'fail_cnt' in meta else 0) + 1

            if fail_cnt > req_cnt:
                key = 'http://%s:%d' % (meta['host'], meta['port'])
                self.processed_proxies.add(key)

                # 连续失败多次，需要丢弃该代理服务器
                item = ProxyItem()
                item['host'] = meta['host']
                item['port'] = meta['port']
                item['scheme'] = 'http'
                item['action'] = 'discard'
                yield item
            else:
                new_meta = meta.copy()
                new_meta['fail_cnt'] = fail_cnt
                yield Request(url='http://www.baidu.com', callback=self.verify, dont_filter=True,
                              errback=self.verify_errback, meta=new_meta)

    def verify(self, response):
        meta = response.meta

        host = meta['host']
        port = meta['port']
        desc = meta['desc']

        settings = self.crawler.settings
        match_word = settings.get('YOUDAILI_MATCH', '')

        if match_word and match_word not in response.body:
            return

        # 尝试三次，取latency的平均值
        req_cnt = settings.getint('YOUDAILI_REQUEST_CNT', 5)
        results = meta['latency_results']
        results.append(meta['download_latency'])

        if len(results) < req_cnt:
            new_meta = meta.copy()
            new_meta['latency_results'] = results
            yield Request(url='http://www.baidu.com', callback=self.verify, dont_filter=True,
                          errback=self.verify_errback, meta=new_meta)
        else:
            key = 'http://%s:%d' % (host, port)
            self.processed_proxies.add(key)

            item = ProxyItem()

            item['host'] = host
            item['port'] = port
            item['scheme'] = 'http'
            item['desc'] = desc
            item['latency'] = sum(results) / len(results)
            item['validate_time'] = datetime.utcnow()
            item['validate_by'] = 'baidu'

            if meta['update_existing']:
                item['action'] = 'update_latency'
            else:
                item['action'] = 'default'

            yield item
