# coding=utf-8

import json
from urlparse import urljoin
import re
import logging
import scrapy
from scrapy.http import Request
from scrapy.selector import Selector

from andaman.utils.html import html2text, parse_time
from andaman.items.pintour import PintourItem


class PintourSpider(scrapy.Spider):
    name = 'pintour'
    allowed_domains = ['pintour.com']

    def start_requests(self):
        for i in range(1, 12):
            url = 'http://www.pintour.com/list/0-0-0-0-2-1-s-0_%d' % i
            yield scrapy.Request(url)

    def parse(self, response):
        metalist = Selector(text=response.body).xpath('//ul[@class="mateList"]/li/div/h3/a/@href').extract()
        for href in metalist:
            tid = int(href[1:])
            url = 'http://www.pintour.com/%d' % tid
            yield Request(url, callback=self.parse_dir_contents)

    def parse_dir_contents(self, response):
        item = PintourItem()
        item['tid'] = int(response.url.split('/')[3])
        item['title'] = response.xpath('//title/text()').extract()[0]
        data = response.xpath('//div[@class="colBox clearfix"]')[0]
        item['author'] = data.xpath('//div[@class="colBoxL clearfix"]/dl/dt/a/text()').extract()[0]
        item['author_avatar'] = data.xpath('//div[@class="colBoxL clearfix"]/a/img/@src').extract()[0]
        item['type'] = data.xpath('//div[@class="colBoxR"]/div//a/span/text()').extract()
        time = data.xpath('.//div[@class="timePlace clearfix"]/p/text()').extract()[0]
        item['start_time'] = time
        item['departure'] = data.xpath('.//div[@class="timePlace clearfix"]/p[@class="plrCon"]/a/text()').extract()[0]
        item['destination'] = data.xpath('.//div[@class="timePlace clearfix"]/p[@class="plrCon"]/a/text()').extract()
        del item['destination'][0]
        item['description'] = ' '.join(
            filter(lambda v: v, [tmp.strip() for tmp in data.xpath('//div[@class="colBoxB"]//text()').extract()]))
        item['comments'] = []

        if re.search(r'\d+条回应', response.body):
            reply_num = int(re.search(r'\d+条回应', response.body).group(0)[:-9])
            total = reply_num / 20 + 1
            url = 'http://www.pintour.com/%d_1' % item['tid']
            yield Request(url,
                          meta={'item': item, 'page': 1, 'total': total, 'tid': item['tid']}, callback=self.parse_comments)

    def parse_comments(self, response):
        item = response.meta['item']
        page = response.meta['page'] + 1
        for node in response.xpath('//ul[@class="reply"]/li'):
            author = node.xpath('.//div/input/@value').extract()[0]
            author_avatar = node.xpath('.//a/img/@src').extract()[0]
            comment = node.xpath('.//div/input/@value').extract()[2]
            cid = int(node.xpath('.//div/@class').extract()[0].encode('UTF-8')[10:])
            comment_item = {'cid': cid, 'author_avatar': author_avatar, 'author': author, 'comment': comment}
            item['comments'].append(comment_item)

        if page <= response.meta['total']:
            url = 'http://www.pintour.com/%d_%d' % (item['tid'], page)
            yield Request(url, meta={'item': item, 'page': page, 'total': response.meta['total']},
                          callback=self.parse_comments)
        else:
            yield item