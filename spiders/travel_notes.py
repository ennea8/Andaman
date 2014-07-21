# coding=utf-8
import re
from items import TravelNotesItem

__author__ = 'Jerry'

import scrapy


class TravelNote(scrapy.Spider):
    name = 'notes'
    start_urls = ['http://lvyou.baidu.com/notes/b758b7ddf32d78c54d884ceb/d']

    def parse(self, response):
        item = TravelNotesItem()

        item['user_name'] = response.xpath \
            ('//a[contains(@href, "/user")]/text()')[0].extract()
        item['user_url'] = 'http://lvyou.baidu.com%s' % \
                           response.xpath('//a[contains(@href, "/user")]/@href')[0].extract()

        infos = response.xpath \
            ('//span[contains(@class, "infos")]/text()').extract()
        start_time = str(infos[0])
        item['start_year'] = re.search("([\d]*)(\D*)(\d*)(\D*)", start_time).group(1)
        item['start_month'] = re.search("(\d*)(\D*)(\d*)(\D*)", start_time).group(3)

        item['origin'] = infos[1][1:]
        item['destination'] = infos[3]
        item['time'] = infos[4]
        item['cost'] = re.search("\D*(\d*\D*\d*)\D*", str(infos[5])).group(1)

        item['quality'] = response.xpath \
            ('//div[contains(@class, "notes-stamp")]/img/@title').extract()

        item['title'] = response.xpath \
            ('//span[contains(@id, "J_notes-title")]/text()').extract()
        # sub_title
        sub_title = response.xpath \
            ('//span[contains(@class, ""path-name path-nslog-name nslog"")]/text()').extract()
        # path
        path = response.xpath \
            ('//span[contains(@class, ""path-detail"")]/@span').extract()
        # link
        link = response.xpath \
            ('//span[contains(@class, ""path-name path-nslog-name nslog"")]/@href').extract()
        ## <span class="secondary">发表于2014-07-09 15:16</span>
        ## <div class="html-content">

        item['content'] = {}

        rep_view = str(response.xpath \
                           ('//div[contains(@class, "fl")]/text()').extract())
        item['reply'] = re.search("\D*(\d*)\D*(\d*)\D*", rep_view).group(1)
        item['view'] = re.search("\D*(\d*)\D*(\d*)\D*", rep_view).group(2)

        item['recommend'] = response.xpath \
            ('//em[contains(@class, "recommend")]/text()')[0].extract()
        item['favourite'] = response.xpath \
            ('//em[contains(@class, "favorite-btn-count")]/text()')[0].extract()

        print item['user_name']
        print item['user_url']
        print 'start_year', item['start_year']
        print 'start_month', item['start_month']
        print item['title']

        print 'quality', item['quality']
        print item['reply']
        print item['view']
        print item['recommend']
        print item['favourite']

        yield item



