# coding=utf-8

import json
from urlparse import urljoin
import re
import logging
import scrapy
from scrapy.http import Request
from scrapy.selector import Selector

from andaman.utils.html import html2text, parse_time
from andaman.items.jieban import JiebanItem


class YoubanSpider(scrapy.Spider):
    name = 'yueban'

    def start_requests(self):
        # total_page = self.crawler.settings.getint('YOUBAN_JIEBAN_PAGES', 10)
        total_page = 2
        for i in range(1, total_page):
            url = 'http://yueban.com/menu/%d' % (20 * i)
            yield scrapy.Request(url)

    def parse(self, response):
        item = JiebanItem()
        item['source'] = 'yueban'
        metalist = Selector(text=response.body).xpath('//div[@class="topic-list"]/div/div/div[@class="section"]')
        for sec in metalist:
            item['tid'] = sec.xpath('.//div[@class="section-header"]/a/@href').re(r'\d+')[0]
            item['title'] = ''.join(filter(lambda v: v, [tmp.strip() for tmp in sec.xpath(
                    './/div[@class="section-header"]/a/descendant-or-self::text()').extract()]))
            item['destination'] = ''.join(filter(lambda v: v, [tmp.strip() for tmp in sec.xpath(
                    './/div[@class="section-header"]/p/descendant-or-self::text()').extract()]))
            pass
        item['description'] = ''.join(filter(lambda v: v, [tmp.strip() for tmp in Selector(text=response.body).xpath(
            '//div[@class="topic-list"]/div/div/div/div[@class="section-content"]/text()').extract()]))
        yield item
