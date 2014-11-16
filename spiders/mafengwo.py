# coding=utf-8
import re

from scrapy import Item, Request, Field, Selector, log

from spiders import AizouCrawlSpider


__author__ = 'zephyre'


class MafengwoMddItem(Item):
    data = Field()
    type = Field()


class MafengwoMddSpider(AizouCrawlSpider):
    """
    马蜂窝目的地的抓取
    """

    name = 'mafengwo-mdd'

    def __init__(self, *a, **kw):
        self.name = 'weather'
        super(MafengwoMddSpider, self).__init__(*a, **kw)

    def start_requests(self):
        urls = [
            'http://www.mafengwo.cn/jd/52314/',  # 亚洲
            'http://www.mafengwo.cn/jd/10853/gonglve.html',  # 南极州
            'http://www.mafengwo.cn/jd/14701/gonglve.html',  # 大洋洲
            'http://www.mafengwo.cn/jd/14517/gonglve.html',  # 非洲
            'http://www.mafengwo.cn/jd/14383/gonglve.html',  # 欧洲
            'http://www.mafengwo.cn/jd/16406/gonglve.html',  # 南美
            'http://www.mafengwo.cn/jd/16867/gonglve.html',  # 北美
        ]
        return [Request(url=url) for url in urls]

    def parse(self, response):
        for node in Selector(response).xpath('//dd[@id="region_list"]/a[@href]'):
            url = self.build_href(response.url, node.xpath('./@href').extract()[0])
            yield Request(url=url, callback=self.parse_mdd, meta={'type': 'region'})

    def parse_poi_list(self, response):
        """
        解析页面内的poi列表
        :param response:
        :return:
        """
        poi_type = response.meta['type']
        for href in Selector(response).xpath(
                '//ul[@class="poi-list"]/li[contains(@class,"item")]/div[@class="title"]//a[@href]/@href').extract():
            yield Request(self.build_href(response.url, href), meta={'type': poi_type}, callback=self.parse_poi)

    def parse_mdd(self, response):
        sel = Selector(response)
        ctype = response.meta['type']

        # 继续抓取下级的region
        for node in sel.xpath('//dd[@id="region_list"]/a[@href]'):
            url = self.build_href(response.url, node.xpath('./@href').extract()[0])
            yield Request(url=url, callback=self.parse_mdd, meta={'type': ctype})

        results = self.parse_poi_list(response)
        if hasattr(results, '__iter__'):
            for entry in results:
                yield entry
        elif isinstance(results, Request):
            yield results

        # poi列表的翻页
        for href in sel.xpath('//div[@class="page-hotel"]/a[@href]/@href').extract():
            yield Request(self.build_href(response.url, href), callback=self.parse_poi_list, meta={'type': ctype})

        # 根据url判断是否为poi
        if re.search(r'poi/\d+.html', response.url):
            results = self.parse_poi(response)
            if hasattr(results, '__iter__'):
                for entry in results:
                    yield entry
            elif isinstance(results, Request):
                yield results
            return

        item = MafengwoMddItem()
        data = {'id': int(re.search(r'/(\d+)(/gonglve.html|/)?$', response.url).group(1))}
        item['data'] = data
        item['type'] = ctype

        # 获得crumb
        crumb = []
        for node in sel.xpath(
                '//div[@class="crumb"]/div[contains(@class,"item")]/div[@class="drop"]/span[@class="hd"]/a[@href]'):
            crumb_name = node.xpath('./text()').extract()[0].strip()
            crumb_url = self.build_href(response.url, node.xpath('./@href').extract()[0])
            if not re.search(r'travel-scenic-spot/mafengwo/\d+\.html', crumb_url):
                continue
            crumb.append({'name': crumb_name, 'url': crumb_url})
        data['crumb'] = crumb

        col_list = []
        for node in sel.xpath('//ul[@class="nav-box"]/li[contains(@class,"nav-item")]/a[@href]'):
            info_title = node.xpath('./text()').extract()[0]
            next_url = self.build_href(response.url, node.xpath('./@href').extract()[0])
            # 根据是否出现国家攻略来判断是否为国家
            if info_title == u'国家概况' or info_title == u'城市概况':
                col_list.extend(
                    [(self.build_href(response.url, tmp), self.parse_info, {'type': 'region'}) for tmp in
                     node.xpath('..//dl/dt/a[@href]/@href').extract()])
                if info_title == u'国家概况':
                    data['type'] = 'country'
                elif info_title == u'城市概况':
                    data['type'] = 'region'
                else:
                    self.log(u'Invalid MDD type: %s, %s' % (info_title, response.url), log.WARNING)
                    return

            elif info_title == u'购物':
                yield Request(url=next_url, callback=self.parse_mdd, meta={'type': 'gw'})
            elif info_title == u'娱乐':
                yield Request(url=next_url, callback=self.parse_mdd, meta={'type': 'yl'})
            elif info_title == u'美食':
                yield Request(url=next_url, callback=self.parse_mdd, meta={'type': 'cy'})

        col_url, cb, meta = col_list[0]
        col_list = col_list[1:]
        meta['col_list'] = col_list
        meta['item'] = item
        yield Request(url=col_url, callback=cb, meta=meta)

    def parse_poi(self, response):
        return []

    def parse_info(self, response):
        sel = Selector(response)
        item = response.meta['item']
        data = item['data']

        if 'contents' not in data:
            data['contents'] = []
        contents = data['contents']
        entry = {}
        for node in sel.xpath('//div[@class="content"]/div[@class]'):
            class_name = node.xpath('./@class').extract()[0]
            if 'm-subTit' in class_name:
                contents.append(entry)
                entry = {'title': node.xpath('./h2/text()').extract()[0].strip()}
            elif 'm-txt' in class_name or 'm-img' in class_name:
                entry['txt'] = node.extract().strip()
            else:
                continue
        contents.append(entry)
        data['contents'] = filter(lambda val: val, contents)

        col_list = response.meta['col_list']
        if col_list:
            col_url, cb, meta = col_list[0]
            col_list = col_list[1:]
            meta['col_list'] = col_list
            meta['item'] = item
            yield Request(url=col_url, callback=cb, meta=meta)
        else:
            yield item