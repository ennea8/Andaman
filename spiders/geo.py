# coding=utf-8
import re

import pymongo
from scrapy import Request, Selector, Item, Field
from scrapy.contrib.spiders import CrawlSpider


__author__ = 'zephyre'


class QyerCountryItem(Item):
    # 国家id
    country_id = Field()
    # 国家中文名称
    country_zh = Field()
    # 国家英文名称
    country_en = Field()
    # 洲中文名称
    cont_zh = Field()
    # 洲英文名称
    cont_en = Field()
    # 是否为热本国家
    is_hot = Field()
    url = Field()


class QyerLocItem(Item):
    # 国家id
    country_id = Field()
    # 国家英文名称
    country_en = Field()
    # 洲中文名称
    cont_zh = Field()
    # 洲英文名称
    cont_en = Field()
    # 是否为热本国家
    is_hot = Field()
    url = Field()


class QyerCountrySpider(CrawlSpider):
    name = 'qyer_countries'  # name of spider

    def __init__(self, *a, **kw):
        super(QyerCountrySpider, self).__init__(*a, **kw)

    def start_requests(self):
        yield Request(url='http://place.qyer.com', callback=self.parse)

    def parse(self, response):
        for node in Selector(response).xpath('//div[contains(@class,"pla_indcountrylist") and @id]'):
            ret = node.xpath('./h2[contains(@class,"title")]/em/a/text()').extract()
            if not ret:
                continue
            ret = re.split(r'\s+', ret[0])
            cn_name = ret[0]
            en_name = ' '.join(ret[1:])

            for node_c in node.xpath('./div[contains(@class,"line")]/ul/li[@class="item"]//a[@data-bn-ipg]'):
                url = node_c.xpath('./@href').extract()[0]
                match = re.search(r'place-index-countrylist-(\d+)', node_c.xpath('./@data-bn-ipg').extract()[0])
                if not match:
                    continue
                country_id = int(match.groups()[0])
                ret = node_c.xpath('./text()').extract()
                if not ret:
                    continue
                country_zh = ret[0].strip()
                ret = node_c.xpath('./span[@class="en"]/text()').extract()
                if not ret:
                    continue
                country_en = ret[0].strip()
                ret = node_c.xpath('../../p[contains(@class,"hot")]')
                is_hot = bool(ret)

                item = QyerCountryItem()
                item['country_id'] = country_id
                item['country_zh'] = country_zh
                item['country_en'] = country_en
                item['cont_zh'] = cn_name
                item['cont_en'] = en_name
                item['is_hot'] = is_hot
                item['url'] = url

                yield item


class QyerCountryPipeline(object):
    spiders = [QyerCountrySpider.name]

    def process_item(self, item, spider):
        if type(item).__name__ != QyerCountryItem.__name__:
            return item

        cid = item['country_id']
        col = pymongo.Connection().raw_data.QyerCountry
        # NoSQL
        entry = col.find_one({'countryId': cid})
        if not entry:
            entry = {}

        entry['countryId'] = cid
        entry['zhName'] = item['country_zh']
        entry['enName'] = item['country_en']
        entry['zhContinent'] = item['cont_zh']
        entry['enContinent'] = item['cont_en']
        entry['isHot'] = item['is_hot']
        entry['url'] = item['url']

        col.save(entry)
        return item


class QyerCountryProcSpider(CrawlSpider):
    """
    对穷游的国家数据进行清洗
    """
    name = 'qyer_countries_proc'  # name of spider

    def __init__(self, *a, **kw):
        super(QyerCountryProcSpider, self).__init__(*a, **kw)

    def start_requests(self):
        yield Request(url='http://www.baidu.com', callback=self.parse)

    def parse(self, response):
        for entry in pymongo.Connection().raw_data.QyerCountry.find({}):
            item = QyerCountryItem()
            item['country_id'] = entry['countryId']
            item['country_zh'] = entry['zhName']
            item['country_en'] = entry['enName']
            item['cont_zh'] = entry['zhContinent']
            item['cont_en'] = entry['enContinent']
            item['is_hot'] = entry['isHot']

            yield item


class QyerCountryProcPipeline(object):
    """
    对穷游的国家数据进行清洗
    """

    spiders = [QyerCountryProcSpider.name]

    def process_item(self, item, spider):
        if type(item).__name__ != QyerCountryItem.__name__:
            return item

        col = pymongo.Connection().geo.Country
        zh_name = item['country_zh']
        entry = col.find_one({'zhName': zh_name})
        if not entry:
            entry = {}

        entry['zhName'] = zh_name
        entry['enName'] = item['country_en']
        entry['qyerId'] = item['country_id']
        entry['zhCont'] = item['cont_zh']
        entry['enCont'] = item['cont_en']
        entry['isHot'] = item['is_hot']

        col.save(entry)

        return item


class TravelGisSpider(CrawlSpider):
    name = 'travel_gis'  # name of spider

    def __init__(self, *a, **kw):
        super(TravelGisSpider, self).__init__(*a, **kw)

    def start_requests(self):
        yield Request(url='http://www.travelgis.com/world/ppla.asp?pagenumber=1', callback=self.parse)

    def parse(self, response):
        # for node in Selector(response)
        pass