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


class CtripSpider(scrapy.Spider):

    name = 'ctrip'

    def start_requests(self):
        start_urls = [
            'http://vacations.ctrip.com/tours',
            'http://vacations.ctrip.com/tours/inter'
        ]
        for url in start_urls:
            yield Request(url)

    def parse(self, response):
        for city in response.xpath('//div[@class="sel_list"]/dl/dd/a/@href').extract():
            num = int(re.search(r'\d+',str(city)).group(0))
            url = 'http://you.ctrip.com/DangdiSite/events/%d.html' % num
            yield Request(url, callback=self.parse_city)

    def parse_city(self, response):
        pass
