# coding=utf-8
import copy
import re

import datetime
from scrapy import Item, Field, Request, Selector
from scrapy.contrib.spiders import CrawlSpider

from utils import get_mongodb


__author__ = 'zephyre'


class ProxyItem(Item):
    scheme = Field()
    host = Field()
    port = Field()
    user = Field()
    passwd = Field()
    desc = Field()
    latency = Field()

    # 发布时间
    publishTime = Field()

    # 验证时间
    verifiedTime = Field()

    # 通过哪些渠道进行过验证？
    verifyMethods = Field()

    # 是否通过验证
    verified = Field()


class BaseProxySpider(CrawlSpider):
    def __init__(self, *a, **kw):
        super(BaseProxySpider, self).__init__(*a, **kw)

        self.verify_map = {
            'baidu': {'url': 'http://www.baidu.com',
                      'callback': lambda response: self.verify_base(response, '<title>百度一下，你就知道</title>')},
            'googleapis': {'url': 'http://maps.googleapis.com/maps/api/geocode/json?address=1600+Amphitheatre+Parkway',
                           'callback': lambda response: self.verify_base(response, 'address_components')}}

    def start_requests(self):
        raise NotImplementedError

    def next_req(self, meta):
        """
        逐个根据verifier构造HTTP请求
        :param meta:
        :param proxy:
        :return:
        """
        item = meta['item'] if 'item' in meta else None
        proxy = meta['proxy'] if 'proxy' in meta else None
        verifier = meta['verifier'] if 'verifier' in meta else None

        if not verifier:
            return item

        verifier = copy.copy(verifier)
        v = verifier.pop()

        meta = {}
        if item:
            meta['item'] = item
        if proxy:
            meta['proxy'] = proxy
        meta['verifier'] = verifier
        meta['verifier_head'] = v
        return Request(url=self.verify_map[v]['url'], callback=self.verify_map[v]['callback'], meta=meta,
                       dont_filter=True, errback=self.verifail)

    def parse(self, response):
        raise NotImplementedError

    def verifail(self, response):
        item = response.request.meta['item']
        verifier = response.request.meta['verifier']
        verifier_head = response.request.meta['verifier_head']
        proxy = response.request.meta['proxy']

        item['verifiedTime'] = datetime.datetime.now()
        if 'verified' not in item or not item['verified']:
            item['verified'] = {}
        item['verified'][verifier_head] = False

        meta = {'item': item, 'proxy': proxy, 'verifier': verifier}
        yield self.next_req(meta)

    def verify_base(self, response, keyword):
        item = response.request.meta['item']
        verifier = response.request.meta['verifier']
        verifier_head = response.request.meta['verifier_head']
        proxy = response.request.meta['proxy']

        if 'latency' not in item or not item['latency']:
            item['latency'] = {}
        item['latency'][verifier_head] = response.meta['download_latency']
        item['verifiedTime'] = datetime.datetime.now()
        if 'verified' not in item or not item['verified']:
            item['verified'] = {}
        item['verified'][verifier_head] = (response.status == 200
                                           and keyword in response.body)

        meta = {'item': item, 'proxy': proxy, 'verifier': verifier}
        yield self.next_req(meta)


class YoudailiProxySpider(BaseProxySpider):
    name = 'youdaili_proxy'

    def __init__(self, *a, **kw):
        super(YoudailiProxySpider, self).__init__(*a, **kw)

    def start_requests(self):
        verifier = []
        if 'param' in dir(self):
            param = getattr(self, 'param', {})
            if 'verify' in param:
                if 'baidu' in param['verify']:
                    verifier.append('baidu')
                if 'googleapis' in param['verify']:
                    verifier.append('googleapis')

        if not verifier:
            return

        tmp = []
        for k in verifier:
            tmp.append({'verified.%s' % k: True})
        if not tmp:
            query = {}
        elif len(tmp) == 1:
            query = tmp[0]
        else:
            query = {'$or': tmp}
        for entry in get_mongodb('misc', 'Proxy', profile='mongodb-general').find(query):
            item = ProxyItem()
            item['host'] = entry['host']
            item['port'] = entry['port']
            item['desc'] = entry['desc']
            item['scheme'] = 'http'

            proxy = '%s://%s:%d' % (item['scheme'], entry['host'], entry['port'])

            meta = {'item': item, 'proxy': proxy, 'verifier': verifier}
            yield self.next_req(meta)

        template = 'http://www.youdaili.net/Daili/http/list_%d.html'
        try:
            max_page = int(getattr(self, 'param', {})['max-page'][0])
        except (KeyError, ValueError, IndexError):
            max_page = 1

        # inclusive
        for page in xrange(1, max_page + 1):
            yield Request(url=template % page, callback=self.parse,
                          meta={'verifier': verifier})

    def parse(self, response):  # draw the state
        for node in Selector(response).xpath('//div[@class="newslist_body"]/ul[@class="newslist_line"]/li'):
            tmp = node.xpath('./a[@href]/@href').extract()
            if not tmp:
                continue
            href = tmp[0]

            yield Request(url=href, callback=self.parse_proxylist,
                          meta={'verifier': response.meta['verifier']})

    def parse_proxylist(self, response):
        sel = Selector(response)
        for page_node in sel.xpath('//div[@class="dede_pages"]/ul[@class="pagelist"]/li/a[@href]'):
            tmp = page_node.xpath('./text()').extract()
            try:
                int(tmp[0])
            except ValueError:
                continue

            tmp = page_node.xpath('./@href').extract()
            if not tmp:
                continue
            page_href = tmp[0]
            if page_href.strip() == '#':
                continue

            m = re.search(r'^(.+)/[^/]+$', response.url)
            if not m:
                continue
            url = '%s/%s' % (m.groups()[0], page_href)
            yield Request(url=url, callback=self.parse_proxylist,
                          meta={'verifier': response.meta['verifier']})

        for entry in sel.xpath('//div[@class="cont_font"]/p/text()').extract():
            entry = entry.strip()
            idx = entry.find('#')
            desc = entry[idx + 1:] if idx != -1 else ''
            entry = entry[:idx] if idx != -1 else entry
            idx = entry.find('@')
            if idx != -1:
                entry = entry[:idx]
            tmp = entry.split(':')
            if len(tmp) < 2:
                continue
            host, port = tmp[:2]
            try:
                port = int(port)
            except ValueError:
                continue

            item = ProxyItem()
            item['host'] = host
            item['port'] = port
            item['desc'] = desc
            item['scheme'] = 'http'

            proxy = '%s://%s:%d' % (item['scheme'], host, port)

            meta = {'item': item, 'proxy': proxy, 'verifier': response.meta['verifier']}
            yield self.next_req(meta)


class ProxyPipeline(object):
    # 向pipline注册
    spiders = [YoudailiProxySpider.name]

    def process_item(self, item, spider):
        col = get_mongodb('misc', 'Proxy', profile='mongodb-general')
        data = col.find_one({'host': item['host'], 'port': item['port']})
        if not data:
            data = {}

        # 和原有数据合并
        for k in item:
            if k == 'latency':
                if k in data:
                    for latency_key in item[k]:
                        data[k][latency_key] = item[k][latency_key]
                else:
                    data[k] = item[k]
            elif k == 'verifyMethods':
                if k in data:
                    method_set = set(data[k])
                    for m in item[k]:
                        method_set.add(m)
                    data[k] = list(method_set)
                else:
                    data[k] = item[k]
            elif k == 'verified':
                if k in data:
                    for verifier in item[k]:
                        data[k][verifier] = item[k][verifier]
                else:
                    data[k] = item[k]
            else:
                data[k] = item[k]

        col.save(data)

        return item


