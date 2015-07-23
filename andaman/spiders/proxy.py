# coding=utf-8
from urlparse import urljoin
import re
import time

import scrapy
from scrapy.http import Request

from andaman.items.proxy import ProxyItem


__author__ = 'zephyre'


class YoudailiSpider(scrapy.Spider):
    name = 'youdaili'

    start_urls = ['http://www.youdaili.net/Daili/guonei/list_%d.html' % page for page in [1, 2]]

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

                yield Request(url='http://www.baidu.com', callback=self.verify, dont_filter=True,
                              errback=self.verify_errback,
                              meta={'host': host, 'port': port, 'desc': desc, 'latency_results': [],
                                    'proxy': 'http://%s:%d' % (host, port),
                                    'dont_redirect': True, 'dont_retry': True, 'download_timeout': 10})

    @staticmethod
    def verify_errback(response):
        pass

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
        results = meta['latency_results']
        results.append(meta['download_latency'])
        if len(results) < 3:
            yield Request(url='http://www.baidu.com', callback=self.verify, dont_filter=True,
                          errback=self.verify_errback,
                          meta={'host': host, 'port': port, 'desc': desc, 'latency_results': results,
                                'proxy': 'http://%s:%d' % (host, port),
                                'dont_redirect': True, 'dont_retry': True, 'download_timeout': 10})
        else:
            item = ProxyItem()

            item['host'] = host
            item['port'] = port
            item['scheme'] = 'http'
            item['desc'] = desc
            item['latency'] = sum(results) / len(results)
            item['verifiedTime'] = long(time.time() * 1000)

            yield item
