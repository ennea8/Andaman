# coding=utf-8
import copy
import re

import datetime
from scrapy import Item, Field, Request, Selector

from middlewares import ProxySwitchMiddleware
from spiders import AizouCrawlSpider, AizouPipeline


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


class BaseProxySpider(AizouCrawlSpider):
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

        item['verifiedTime'] = datetime.datetime.utcnow()
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
        item['verifiedTime'] = datetime.datetime.utcnow()
        if 'verified' not in item or not item['verified']:
            item['verified'] = {}
        item['verified'][verifier_head] = (response.status == 200
                                           and keyword in response.body)

        meta = {'item': item, 'proxy': proxy, 'verifier': verifier}
        yield self.next_req(meta)


class YoudailiProxySpider(BaseProxySpider):
    name = 'youdaili-proxy'
    uuid = '8201b0c5-cbcc-4426-9b05-d24e79619809'

    def start_requests(self):
        verifier = []
        if 'verify' in self.param:
            if 'baidu' in self.param['verify']:
                verifier.append('baidu')
            if 'googleapis' in self.param['verify']:
                verifier.append('googleapis')

        if not verifier:
            return

        template = 'http://www.youdaili.net/Daili/http/list_%d.html'
        try:
            max_page = int(getattr(self, 'param', {})['max-page'][0])
        except (KeyError, ValueError, IndexError):
            max_page = 1

        # inclusive
        for page in xrange(1, max_page + 1):
            yield Request(url=template % page, callback=self.parse, meta={'verifier': verifier})

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


class DBProxySpider(BaseProxySpider):
    name = 'db-proxy'
    uuid = '039300bb-d4a7-4dfd-9437-03fa5b281627'

    def start_requests(self):
        verifier = []

        if 'verify' in self.param:
            if 'baidu' in self.param['verify']:
                verifier.append('baidu')
            if 'googleapis' in self.param['verify']:
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
        for entry in self.fetch_db_col('misc', 'Proxy', 'mongodb-general').find(query):
            item = ProxyItem()
            item['host'] = entry['host']
            item['port'] = entry['port']
            item['desc'] = entry['desc'] if 'desc' in entry else None
            item['scheme'] = entry['scheme'] if 'scheme' in entry else 'http'
            item['user'] = entry['user'] if 'user' in entry else None
            item['passwd'] = entry['passwd'] if 'passwd' in entry else None

            if item['user'] and item['passwd']:
                proxy = '%s://%s:%s@%s:%d' % (item['scheme'], item['user'], item['passwd'], item['host'], item['port'])
            else:
                proxy = '%s://%s:%d' % (item['scheme'], item['host'], item['port'])

            meta = {'item': item, 'proxy': proxy, 'verifier': verifier}
            yield self.next_req(meta)


class FreeListProxySpider(BaseProxySpider):
    name = 'freelist-proxy'
    uuid = '61ebc6ae-565c-4f72-ac79-b2b5106ad9ef'

    def start_requests(self):
        verifier = []
        path = None

        if 'verify' in self.param:
            if 'baidu' in self.param['verify']:
                verifier.append('baidu')
            if 'googleapis' in self.param['verify']:
                verifier.append('googleapis')
        if 'path' in self.param and self.param['path']:
            path = self.param['path'][0]

        if not verifier:
            return

        if path:
            # 从本地文件读取
            import os

            abs_path = os.path.normpath(os.path.join(os.getcwd(), path))
            yield Request(url='file://%s' % abs_path, callback=self.parse, meta={'verifier': verifier})
        else:
            ps = ProxySwitchMiddleware(verifier[0], 10, 12, 100)
            proxy = ps.pick_proxy()
            template = 'http://free-proxy-list.net/%s-proxy.html'
            for country in ['us', 'uk']:
                yield Request(url=template % country, callback=self.parse, meta={'verifier': verifier, 'proxy': proxy})

    def parse(self, response):  # draw the state
        for node in Selector(response).xpath('//table[@id="proxylisttable"]/tbody/tr'):
            vals = node.xpath('./td/text()').extract()
            if len(vals) != 8:
                continue

            host = vals[0]
            if not re.search(
                    r'(^[2][5][0-5]\.|^[2][0-4][0-9]\.|^[1][0-9][0-9]\.|^[0-9][0-9]\.|^[0-9]\.)([2][0-5][0-5]\.|[2][0-4][0-9]\.|[1][0-9][0-9]\.|[0-9][0-9]\.|[0-9]\.)([2][0-5][0-5]\.|[2][0-4][0-9]\.|[1][0-9][0-9]\.|[0-9][0-9]\.|[0-9]\.)([2][0-5][0-5]|[2][0-4][0-9]|[1][0-9][0-9]|[0-9][0-9]|[0-9])$',
                    host):
                continue
            try:
                port = int(vals[1])
            except ValueError:
                continue
            desc = 'Country: %s, Anonymity: %s, Google: %s, HTTPS: %s' % tuple(vals[3:7])

            item = ProxyItem()
            item['host'] = host
            item['port'] = port
            item['desc'] = desc
            item['scheme'] = 'http'

            proxy = '%s://%s:%d' % (item['scheme'], host, port)

            meta = {'item': item, 'proxy': proxy, 'verifier': response.meta['verifier']}
            yield self.next_req(meta)


class ProxyPipeline(AizouPipeline):
    # 向pipline注册
    spiders = [YoudailiProxySpider.name, DBProxySpider.name, FreeListProxySpider.name]

    spiders_uuid = [YoudailiProxySpider.uuid, DBProxySpider.uuid, FreeListProxySpider.uuid]

    def process_item(self, item, spider):
        if not self.is_handler(item, spider):
            return item

        col = self.fetch_db_col('misc', 'Proxy', 'mongodb-general')
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


